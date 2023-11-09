{{
    config(
        materialized = 'incremental',
        on_schema_change='fail'
    )
}}

WITH src_reviews AS (
    SELECT * from {{ref('src_reviews')}}
)


SELECT 
{{ dbt_utils.generate_surrogate_key(['listing_id', 'review_date', 'reviewer_name', 'review_text']) }} AS review_id,
* 
FROM src_reviews
WHERE review_text is not null
-- jinja if statement:
--    If this is an incremental load then 
--      We are only interested in the records where the review date from the original date is more current than
--        our latest review date in this table
{% if is_incremental() %}
    AND review_date > (select max(review_date) from {{ this }})
{% endif %}
