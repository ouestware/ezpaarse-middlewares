# OpenAlex

This middleware will query OpenAlex API using the ec DOI field as an identifier. It enriches the ec with some field from OpenAlex data.

## Configuration

### Data enrichment

You can chose Open Alex fields to retrieve to enrich your logs among this fixed list:

- oa_id: OpenAlex internal identifier
- oa_doi: DOI as returned by OpenAlex (always present)
- oa_type: OpenAlex document type
- oa_open_access: OpenAlex open access status
- oa_domains: OpenAlex domain topics by order of importance maximum three
- oa_fields: OpenAlex field topics by order of importance maximum three 
- oa_subfields: OpenAlex subfield topics by order of importance maximum three 
- oa_apc_list: APC price required by editor
- oa_apc_paid: APC price actually paid by authors
- oa_cited_by_count: OpenAlex estimation on number of citations the document received
- oa_cited_by_api_url: OpenAlex URL to retrieve the list of documents citing the document
- oa_fwci: [The Field-weighted Citation Impact (FWCI)](https://help.openalex.org/hc/en-us/articles/24735753007895-Field-Weighted-Citation-Impact-FWCI)
- oa_funders: list of funder names found in OpenAlex `grants` fields 
- oa_sustainable_development_goals: The United Nations' 17 Sustainable Development Goals are a collection of goals at the heart of a global "shared blueprint for peace and prosperity for people and the planet."

Please refer to [OpenAlex documentation](https://docs.openalex.org/api-entities/works/work-object) to learn more

To select data field to retrieve use the `openalex-fields` header which can list the desired fields separated by a `|` character.

By default the middleware will retrieve all the fields.

If you need other fields you will need to extend this middleware source code to decide how to fit OpenAlex data model into EC CSV model.

### mailto...
TODO add all config parameters