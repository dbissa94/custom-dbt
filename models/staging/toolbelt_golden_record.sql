{{ config(materialized='table') }}

SELECT DISTINCT ON (company_address_delivery_line_2)
    *
FROM {{ ref('toolbelt_company_format') }}
ORDER BY company_address_delivery_line_2