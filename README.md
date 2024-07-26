# (WIP -- currently developing on BigQuery only) Customer360 Data Model

## Prerequisites
To use this dbt package, you must have the following:

- At least **TWO** of the following Fivetran Connectors:
  - Marketo
  - Stripe
  - Zendesk

If you are not using one of the above sources, set the respective `customer360__using_<soure>` variable to False:
```yml
# dbt_project.yml
vars:
  customer360__using_stripe: false # default = True
  customer360__using_marketo: false # default = True
  customer360__using_zendesk: false # default = True
```

## How to Run
1. Add the following to your `packages.yml` and comment out any individual references to the Fivetran Stripe, Marketo, and Zendesk packages.
```yml
packages:
  - git: https://github.com/fivetran/dbt_customer360.git # this will install Stripe, Marketo, and Zendesk as dependencies
    revision: main
    warn-unpinned: false
```
2. Execute `dbt deps`
3. Execute `dbt seed -m customer360 --full-refresh`
4. Execute `dbt run -m +customer360` (this will run everything upstream)

## Output
Unified "customer" view.

What is a customer? The lowest grain of individuality (ie individual vs company) of your data from the 3 sources we use:
- Marketo
- Stripe
- Zendesk

Each record where `is_organization_header=False` will represent an Individual (otherwise the most granular level of data present). The package will also insert an organization-level header row for easier aggregations across grains (`is_organization_header=true`) for proper aggregations and flexibility. These will all share the same `customer360_organization_id` but each have a unique `customer360_id`.

> Note: If you are a b2c organization, your source Stripe customer data (and potentially Marketo) may only exist at the organizational level, while other sources (Zendesk and likely Marketo) typically provide customer data at the individual level. See the "Grain of Source Data" section below.

The exact tables:
- `customer360__mapping`: Complete mapping of customer IDs from Marketo, Zendesk, and Stripe onto each other.
- Child informational tables with all values found for a customer across all 3 sources;
  - `customer360__address`
  - `customer360__email`
  - `customer360__phone`
  - `customer360__name`
  - `customer360__organization`
  - `customer360__ip_address`
  - `customer360_status`
  - `customer360__updates`
- A summary table surfacing the most "confident" values (chosen from recency and frequency) from above: `customer360__summary`.
- A customer table aggregating all metrics from Stripe, Marketo, and Zendesk.

## Additional Configurations
### Grain of Source Data
By default, this package assumes each of your data sources presents information at the Individual's level. However, if you are a b2c company, some of your data may exist at the Organization level. This is especially likely of Stripe as opposed to Zendesk and Marketo, in which typically individuals operate.

Tell the package the grain of your source data to better perform identity resolution.
```yml
vars:
  customer360_grain_stripe: organization # default = individual
  customer360_grain_marketo: organization # default = individual
  customer360_grain_zendesk: organization # default = individual
```

### Stripe Individual-Name Configs
Stripe doesn't have distinct name fields for individuals vs organizations. If you store both the indvidual and the company name, and in a consistent enforced format, use the below variables to tell the package how to parse them out from the Stripe `CUSTOMER.customer_name` and `CUSTOMER.shipping_name` fields.

```yml
vars:
    stripe_customer_full_name_extract_sql: "replace( {{ dbt.split_part('customer_name', \"' ('\", 2) }}, ')', '')" # How to extract the individual name from `customer_name`
    stripe_customer_organization_name_extract_sql: "coalesce({{ dbt.split_part('customer_name', \"' ('\", 1) }}, customer_name)" # How to extract the company name from `customer_name`
    stripe_shipping_full_name_extract_sql: "replace( {{ dbt.split_part('shipping_name', \"' ('\", 2) }}, ')', '')" # How to extract the individual name from `shipping_name`
    stripe_shipping_organization_name_extract_sql: "coalesce({{ dbt.split_part('shipping_name', \"' ('\", 1) }}, shipping_name)" # How to extract the company name from `shipping_name`
```

The above example code is intended for environments in which Stripe names are stored as `Company Name (Individual Name)`.

### Leveraging Custom/Internal IDs
By default, the package will perform identity matching based on:
- email
- phone number
- physical address
- individual name (fuzzy matching)
  - organization name is used instead if the `customer360_grain_<source>` variable is set to `organization`.

This heuristic on its own, however, may not offer a high match rate. You may have an interanl ID, such as a product app ID or a third-party tool's ID (ie Salesforce Account ID), that can be leveraged for resolving identities.

```yml
vars:
    customer360_internal_match_ids:
        - name: sf_account_id # to serve as a generalized alias
          customer_grain: individual | organization # should this be applied at the individual or organizational level? affects joins/filters in our identity resolution logic
          marketo: 
            match_key: sfdc_account_id
          stripe:
            match_key: salesforce_account_id
          zendesk:
              map_table: digital-arbor-400.intermediate_tables.salesforce_to_fivetran_account
              source: user | organization # should we use zendesk.USER or zendesk.ORGANIZATION
              join_with_map_on: custom_fivetran_account_id
              map_table_join_on: fivetran_account_id_c
              match_key: account_c

        - name: fivetran_user_id # to serve as a generalized alias
          customer_grain: individual | organization # should this be applied at the individual or organizational level? affects joins/filters in our identity resolution logic
          marketo: 
            match_key: fivetran_user_id
          stripe:
            match_key: fivetran_user_id
          zendesk:
              source: user | organization # should we use zendesk.USER or zendesk.ORGANIZATION
              match_key: fivetran_user_id
```

You can provide multiple "match sets" such as the above. The arguments are as follows:
- `name` (required): General name to serve as an alias for the match field, as its name in each individual source will likely be different.
- `customer_grain` (required): Grain at which to merge customers along this key. Does it apply to entire organizations, or individual consumers? Specify this by providing `individual` or `organization`.
- `source` (required only for Zendesk): What source table the field is coming from. In Zendesk's case, we pull data from both `user` and `organization`, and the `source` should reflect one of these tables.
- `match_key` (required for 2+ sources): What field from each source that the `match_key` is scoped under should be used in identity resolution joins. If you are using a mapping table (see below), this should be the target field from the mapping to use for identity resolution.
- We also allow for the use of an internal intermediate mapping table, in case this is is necessary to grab the necessary `match_key` for all 3 sources. If leveraging a mapping table, add the following arguments:
  - `map_table` (required only if using mapping): The full `<database>.<schema>.<table>` identifier for the mapping table. Do not include quotes. This cannot be a `ref()` :disappointed:
  - `join_with_map_on` (required only if using mapping): Which source field from either `marketo__leads`, `stripe__customer_overview`, `stg_zendesk__user`, or `stg_zendesk__organization` to use for joining with the mapping table.
  - `map_table_join_on`: Which field from the mapping table to use for joining with either `marketo__leads`, `stripe__customer_overview`, `stg_zendesk__user`, or `stg_zendesk__organization`.