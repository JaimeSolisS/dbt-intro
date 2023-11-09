SELECT f.review_date, l.created_at FROM {{ ref('fct_reviews') }} f
JOIN {{ref("dim_listings_cleansed")}} l
ON f.listing_id=l.listing_id
WHERE f.review_date <= l.created_at