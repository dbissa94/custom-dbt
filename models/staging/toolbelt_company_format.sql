-- models/my_model.sql
{{ config(materialized='table') }}

with source_data as (
    SELECT * from {{ ref('toolbelt_company_clean') }}
),
final as (
    select * from source_data
),

updated_data AS (
    select
        id AS id,
    batch_id,
    rn,
    org_id,
    company_id,
    pro_id,
    contact_id,
    company_name,
    company_address_delivery_line_1,
    company_address_delivery_line_2,
--     lower(company_address_delivery_line_2) AS company_address_delivery_line_2,
    company_address_city AS company_address_city,
    company_address_postal_code AS company_address_postal_code,
    company_address_postal_code_plus_4 AS company_address_postal_code_plus_4,
    company_address_rdi,
    company_address_region AS company_address_region,
    company_address_county_fips AS company_address_county_fips,
    company_loc_country AS company_loc_country,
    company_loc_region AS company_loc_region,
    company_loc_region_iso,
    company_loc_region_name AS company_loc_region_name,
    company_loc_region_gnisid AS company_loc_region_gnisid,
    company_loc_postal_code AS company_loc_postal_code,
    company_loc_postal_code_name AS company_loc_postal_code_name,
    company_loc_postal_code_desc AS company_loc_postal_code_desc,
    company_loc_postal_code_longitude AS company_loc_postal_code_longitude,
    company_loc_postal_code_latitude AS company_loc_postal_code_latitude,
    company_loc_place AS company_loc_place,
    company_loc_place_name AS company_loc_place_name,
    company_loc_place_desc AS company_loc_place_desc,
    company_loc_place_longitude AS company_loc_place_longitude,
    company_loc_place_latitude AS company_loc_place_latitude,
    company_loc_county AS company_loc_county,
    company_loc_county_name AS company_loc_county_name,
    company_loc_county_desc AS company_loc_county_desc,
    company_loc_county_fips AS company_loc_county_fips,
    company_loc_csa AS company_loc_csa,
    company_loc_csa_code AS company_loc_csa_code,
    company_loc_csa_population AS company_loc_csa_population,
    company_loc_msa AS company_loc_msa,
    company_loc_msa_population AS company_loc_msa_population,
    company_loc_cbsa_code AS company_loc_cbsa_code,
    company_website,
    company_email,
    CASE
            WHEN company_phone ~ '^\+' THEN company_phone
            ELSE CONCAT('+', company_phone)
        END AS company_phone,
    company_cs,
    company_source_cnt,
    company_source_last_touch,
    contact_name_honorific,
    contact_name_given_name AS contact_name_given_name,
    contact_name_middle_name AS contact_name_middle_name,
    contact_name_surname AS contact_name_surname,
    contact_name_professional_suffix AS contact_name_professional_suffix,
    contact_name_maturity_suffix AS contact_name_maturity_suffix,
    contact_title,
    CASE
            WHEN contact_phone_direct ~ '^\+' THEN contact_phone_direct
            ELSE CONCAT('+', contact_phone_direct)
        END AS contact_phone_direct,
    contact_email,
    contact_email_direct,
    contact_cs,
    contact_source_cnt,
    contact_source_last_touch,
    contact_class_primary,
    contact_class_secondary,
    contact_phone as contact_phone
    from final
)

SELECT *
FROM updated_data
