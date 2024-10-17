const { identity, uniq, keys } = require('lodash');

function getTopicsLabels(openAlexWork, topicLevel) {
  return openAlexWork && openAlexWork.topics
    ? uniq(openAlexWork.topics.map((t) => (t && t[topicLevel] ? t[topicLevel].display_name : null)))
        .filter(identity)
        .join('|')
    : '';
}

const OpenAlexFields = {
  oa_id: {
    openAlexField: 'id',
    getEcData: (openAlexWork) => (openAlexWork ? openAlexWork.id : ''),
  },
  oa_doi: {
    openAlexField: 'doi',
    getEcData: (openAlexWork) =>
      openAlexWork ? openAlexWork.doi.replace('https://doi.org/', '') : '',
  },
  oa_type: {
    openAlexField: 'type',
    getEcData: (openAlexWork) => (openAlexWork ? openAlexWork.type : ''),
  },
  oa_open_access: {
    openAlexField: 'open_access',
    getEcData: (openAlexWork) =>
      openAlexWork && openAlexWork.open_access ? openAlexWork.open_access.oa_status : '',
  },
  oa_domains: {
    openAlexField: 'topics',
    getEcData: (openAlexWork) => getTopicsLabels(openAlexWork, 'domain'),
  },
  oa_fields: {
    openAlexField: 'topics',
    getEcData: (openAlexWork) => getTopicsLabels(openAlexWork, 'field'),
  },
  oa_subfields: {
    openAlexField: 'topics',
    getEcData: (openAlexWork) => getTopicsLabels(openAlexWork, 'subfield'),
  },
  oa_apc_list: {
    openAlexField: 'apc_list',
    getEcData: (openAlexWork) =>
      openAlexWork && openAlexWork.apc_list ? openAlexWork.apc_list.value_usd + '' : '',
  },
  oa_apc_paid: {
    openAlexField: 'apc_paid',
    getEcData: (openAlexWork) =>
      openAlexWork && openAlexWork.apc_paid ? openAlexWork.apc_paid.value_usd + '' : '',
  },
  oa_cited_by_count: {
    openAlexField: 'cited_by_count',
    getEcData: (openAlexWork) =>
      openAlexWork && openAlexWork.cited_by_count ? openAlexWork.cited_by_count + '' : '',
  },
  oa_cited_by_api_url: {
    openAlexField: 'cited_by_api_url',
    getEcData: (openAlexWork) => (openAlexWork ? openAlexWork.cited_by_api_url || '' : ''),
  },
  oa_fwci: {
    openAlexField: 'fwci',
    getEcData: (openAlexWork) => (openAlexWork && openAlexWork.fwci ? openAlexWork.fwci + '' : ''),
  },
  oa_funders: {
    openAlexField: 'grants',
    getEcData: (openAlexWork) =>
      openAlexWork && openAlexWork.grants
        ? uniq(openAlexWork.grants.map((g) => g.funder_display_name))
            .filter(identity)
            .join('|')
        : '',
  },
  oa_sustainable_development_goals: {
    openAlexField: 'sustainable_development_goals',
    getEcData: (openAlexWork) =>
      openAlexWork && openAlexWork.sustainable_development_goals
        ? uniq(openAlexWork.sustainable_development_goals.map((sdg) => sdg.display_name))
            .filter(identity)
            .join('|')
        : '',
  },
  // this is where you can add new custom ec field out of OpenAlex Work Object
};

/**
 * Methods to create EC new data field from an OpenAlex Object
 */
function updateEcWithOpenAlexWork(ec, openAlexWork) {
  keys(OpenAlexFields).forEach((field) => {
    const value = OpenAlexFields[field].getEcData(openAlexWork);
    if (value) ec[field] = value;
  });
}

module.exports = {
  OpenAlexFields,
  updateEcWithOpenAlexWork,
};
