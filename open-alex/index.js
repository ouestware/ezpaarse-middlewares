'use strict';

// deps
const co = require('co');
const request = require('request');
// ezpaarse deps
const cache = ezpaarse.lib('cache')('openalex');
// internal deps
const { doiPattern } = require('./utils');
const { OpenAlexFields, ecFromOpenAlexWork } = require('./open-alex');
const { uniq, values } = require('lodash');

/**
 * Enrich ECs with crossref data
 */
module.exports = function () {
  /**
   * INIT
   */
  const self = this;
  const req = this.request;
  const report = this.report;

  /**
   * CONFIG
   */

  const disabled = /^false$/i.test(req.header('openalex-enrich'));
  const cacheEnabled = !/^false$/i.test(req.header('openalex-cache'));
  let mailto = req.header('openalex-mailto') || 'mailto:ezteam@couperin.org';

  const queryHeaders = {
    'user-agent': `ezPAARSE (https://readmetrics.org; ${mailto})`,
  };
  // Strategy to adopt when an enrichment reaches maxTries : abort, ignore, retry
  let onFail = (req.header('openalex-on-fail') || 'abort').toLowerCase();
  let onFailValues = ['abort', 'ignore', 'retry'];
  const openAlexFieldsHeader = req.header('openalex-fields');
  // default to all defined fields
  let neededOpenAlexFields = values(OpenAlexFields).map((f) => f.openAlexField);
  if (openAlexFieldsHeader) {
    // if a restricted list is provided in header use it but make sure it's a known oa_field
    neededOpenAlexFields = openAlexFieldsHeader
      .split('|')
      .filter((f) => neededOpenAlexFields.includes(f));
  }

  if (onFail && !onFailValues.includes(onFail)) {
    const err = new Error(`openalex-On-Fail should be one of: ${onFailValues.join(', ')}`);
    err.status = 400;
    return err;
  }

  if (disabled) {
    self.logger.verbose('OpenAlex enrichment not activated');
    return function (ec, next) {
      next();
    };
  }

  self.logger.verbose('OpenAlex cache: %s', cacheEnabled ? 'enabled' : 'disabled');

  //new fields created by the middleware
  neededOpenAlexFields.forEach((outputField) => {
    if (this.job.outputFields.added.indexOf(outputField) === -1) {
      this.job.outputFields.added.push(outputField);
    }
  });
  const doiField = req.header('openalex-doi-field') || 'doi';

  // API MANAGEMENT CONFIGURATION
  const ttl = parseInt(req.header('openalex-ttl')) || 3600 * 24 * 7;
  const defaultThrottle = parseInt(req.header('openalex-throttle')) || 200;
  let throttle = defaultThrottle;
  // Maximum number of DOIs to query in a single request
  let packetSize = parseInt(req.header('openalex-paquet-size')) || 100;
  if (packetSize > 100) {
    packetSize = 100;
    self.logger.verbose("OpenAlex paquet size can't be more than 100");
  }
  // Minimum number of ECs to keep before resolving them
  let bufferSize = parseInt(req.header('openalex-buffer-size'));
  // Maximum enrichment attempts
  let maxTries = parseInt(req.header('openalex-max-tries'));
  // Base wait time after a request fails
  let baseWaitTime = parseInt(req.header('openalex-base-wait-time'));

  if (isNaN(bufferSize)) {
    bufferSize = 1000;
  }
  if (isNaN(maxTries)) {
    maxTries = 5;
  }
  if (isNaN(baseWaitTime)) {
    baseWaitTime = 1000;
  }

  const buffer = [];
  let busy = false;
  let finalCallback = null;

  self.logger.verbose('OpenAlex enrichment activated');
  self.logger.verbose('OpenAlex throttle: %dms', throttle);
  self.logger.verbose('OpenAlex paquet size: %d', packetSize);
  self.logger.verbose('OpenAlex buffer size: %d', bufferSize);

  report.set('general', 'openalex-queries', 0);
  report.set('general', 'openalex-fails', 0);
  report.set('general', 'openalex-invalid-dois', 0);

  let minResponseTime = -1;
  let maxResponseTime = -1;
  report.set('general', 'openalex-min-response-time', minResponseTime);
  report.set('general', 'openalex-max-response-time', maxResponseTime);

  return new Promise(function (resolve, reject) {
    cache.checkIndexes(ttl, function (err) {
      if (err) {
        self.logger.error('OpenAlex: failed to ensure indexes');
        return reject(new Error('failed to ensure indexes for the cache of OpenAlex'));
      }

      resolve(process);
    });
  });

  function process(ec, next) {
    if (!ec) {
      finalCallback = next;
      if (!busy) {
        drainBuffer()
          .then(() => {
            finalCallback();
          })
          .catch((err) => {
            this.job._stop(err);
          });
      }
      return;
    }

    buffer.push([ec, next]);

    if (buffer.length > bufferSize && !busy) {
      busy = true;
      self.saturate();

      drainBuffer()
        .then(() => {
          busy = false;
          self.drain();

          if (finalCallback) {
            finalCallback();
          }
        })
        .catch((err) => {
          this.job._stop(err);
        });
    }
  }

  /**
   * Iterate over the buffer, remove ECs with no DOI/PII or cached DOI/PII
   * return a packet of ecs with an uncached DOI
   */
  function getPacket() {
    const packet = {
      ecs: [],
      doi: new Set(),
    };

    return co(function* () {
      while (packet.doi.size < packetSize) {
        const [ec, done] = buffer.shift() || [];
        if (!ec) {
          return packet;
        }
        const doi = ec[doiField];

        if (!doi) {
          done();
          continue;
        }

        if (doi && !doiPattern.test(doi)) {
          report.inc('general', 'openalex-invalid-dois');
          done();
          continue;
        }

        if (doi && cacheEnabled) {
          const cachedDoc = yield checkCache(doi);

          if (cachedDoc) {
            aggregate(cachedDoc, ec);
            done();
            continue;
          }
        }

        packet.ecs.push([ec, done]);
        if (doi) {
          packet.doi.add(doi);
        }
      }

      return packet;
    });
  }

  function checkCache(identifier) {
    return new Promise((resolve, reject) => {
      if (!identifier) {
        return resolve();
      }

      cache.get(identifier.toLowerCase(), (err, cachedDoc) => {
        if (err) {
          return reject(err);
        }
        resolve(cachedDoc);
      });
    });
  }

  function drainBuffer() {
    return co(function* () {
      while (buffer.length >= bufferSize || (finalCallback && buffer.length > 0)) {
        const packet = yield getPacket();

        if (packet.ecs.length === 0 || packet.doi.size === 0) {
          self.logger.silly('OpenAlex: no doi in the paquet');
          yield new Promise((resolve) => {
            setImmediate(resolve);
          });
          continue;
        }

        const results = new Map();
        const identifier = 'doi';

        if (packet[identifier].size === 0) {
          continue;
        }
        let tries = 0;
        let list;

        while (!list) {
          if (tries >= maxTries) {
            if (onFail === 'ignore') {
              self.logger.error(
                `OpenAlex: ignoring packet enrichment after ${maxTries} failed attempts`,
              );
              packet.ecs.forEach(([, done]) => done());
              return;
            }

            if (onFail === 'abort') {
              const err = new Error(`Failed to query OpenAlex ${maxTries} times in a row`);
              return Promise.reject(err);
            }
          }

          yield wait(tries === 0 ? throttle : baseWaitTime * Math.pow(2, tries));

          try {
            list = yield queryOpenAlex(identifier, Array.from(packet[identifier]));
          } catch (e) {
            report.inc('general', 'openalex-fails');
            self.logger.error(`OpenAlex: ${e.message}`);
          }

          tries += 1;
        }

        for (const item of list) {
          let { doi } = item;

          if (doi) {
            doi = doi.toLowerCase().replace('https://doi.org/', '');
            results.set(doi, item);

            try {
              yield cacheResult(doi, item);
            } catch (e) {
              report.inc('general', 'openalex-cache-fail');
            }
          }
        }

        for (const [ec, done] of packet.ecs) {
          if (ec[doiField]) {
            const doi = ec[doiField].toLowerCase();

            if (results.has(doi)) {
              aggregate(results.get(doi), ec);
            } else {
              try {
                yield cacheResult(doi, {});
              } catch (e) {
                report.inc('general', 'openalex-cache-fail');
              }
            }
          }

          done();
        }
      }
    });
  }

  function wait(ms) {
    return new Promise((resolve) => {
      setTimeout(resolve, ms);
    });
  }

  function handleOpenAlexRateLimit(response) {
    //rate limit
    if (response && response.statusCode && response.statusCode === 429) {
      const headers = (response && response.headers) || {};
      const limitHeader = headers['X-RateLimit-Limit'];
      const resetTimeHeader = headers['X-RateLimit-reset'];

      if (response && response.headers && response.headers['x-rateLimit-reset']) {
        const resetTime = response.headers['x-rateLimit-reset'];
        if (resetTime) {
          const resetDate = new Date(resetTime);
          const timeToReset = resetDate.getTime() - Date.now() + 1000;

          // schedule a retry
          // throttle till the reset date given in the API response
          const newThrottle = timeToReset; // in milliseconds
          console.log(`Waiting ${timeToReset / 1000}s till ${resetDate}`);

          if (newThrottle !== throttle) {
            const newRate = Math.ceil((1000 / newThrottle) * 100) / 100;
            const oldRate = Math.ceil((1000 / throttle) * 100) / 100;
            // eslint-disable-next-line max-len
            self.logger.info(
              `OpenAlex: throttle changed from ${throttle}ms (${oldRate}q/s) to ${newThrottle}ms (${newRate}q/s)`,
            );
            throttle = newThrottle;
          }
        }
      } else {
        console.log("Can't retrieve ratelimit_reset: quitting");
        console.log(JSON.stringify(response, null, 2));
        throw new Error("Got 429 error but can't retrieve ratelimit_reset: quitting");
      }

      if (!limitHeader || !resetTimeHeader) {
        return;
      }

      return true;
    }
    return false;
  }

  function handleResponseTime(responseTime) {
    if (minResponseTime < 0 || responseTime < minResponseTime) {
      minResponseTime = responseTime;
      report.set('general', 'openalex-min-response-time', responseTime);
    }
    if (responseTime > maxResponseTime) {
      maxResponseTime = responseTime;
      report.set('general', 'openalex-max-response-time', responseTime);
    }
  }

  function queryOpenAlex(property, values) {
    report.inc('general', 'openalex-queries');

    return new Promise((resolve, reject) => {
      const startTime = Date.now();

      request(
        {
          method: 'GET',
          url: 'https://api.openalex.org/works',
          timeout: 60000,
          headers: queryHeaders,
          json: true,
          qs: {
            // use filter to list the doi we want to retrieve
            //filter=doi:https://doi.org/10.1371/journal.pone.0266781|https://doi.org/10.1371/journal.pone.0267149
            filter: `${property}:${values.join('|')}`,
            // use select to get only the needed field
            select: uniq([...neededOpenAlexFields, 'doi']).join(','),
            // make sure to retrieve the maximum amount of element
            'per-page': packetSize,
          },
        },
        (err, response, body) => {
          handleOpenAlexRateLimit(response);
          handleResponseTime(Date.now() - startTime);

          if (err) {
            return reject(err);
          }

          const status = response && response.statusCode;

          if (!status) {
            return reject(new Error('request failed with no status code'));
          }
          if (status === 401) {
            return reject(new Error('authentication error (is the token valid?)'));
          }
          if (status >= 400) {
            return reject(new Error(`request failed with status ${status}`));
          }

          const list = body && body.message && body.message.results;

          if (!Array.isArray(list)) {
            return reject(new Error('got invalid response from the API'));
          }

          // request succeeded: get throttle to default
          if (throttle !== defaultThrottle) {
            throttle = defaultThrottle;
          }

          return resolve(list);
        },
      );
    });
  }

  function cacheResult(id, item) {
    return new Promise((resolve, reject) => {
      if (!id || !item) {
        return resolve();
      }

      cache.set(id, item, (err, result) => {
        if (err) {
          return reject(err);
        }
        resolve(result);
      });
    });
  }

  function aggregate(item, ec) {
    if (!item) {
      return;
    }
    return { ...ec, ...ecFromOpenAlexWork(item) };
  }
};
