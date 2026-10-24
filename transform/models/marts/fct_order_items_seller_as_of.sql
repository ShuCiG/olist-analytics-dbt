-- Grain: one row per order line item, with the seller version that was current
-- when the order was placed (point-in-time join on dim_sellers_history).
-- fct_order_items.seller_id joins to dim_sellers, which is today's location; this
-- model answers "where was the seller at the time of the sale".
-- Ranges are half-open [valid_from, valid_to): a purchase at the exact moment of a
-- change belongs to the new version, so each item matches at most one version.
with order_items as (
    select * from {{ ref('fct_order_items') }}
),

seller_versions as (
    select * from {{ ref('dim_sellers_history') }}
),

final as (
    select
        order_items.order_item_sk,
        order_items.order_id,
        order_items.seller_id,
        order_items.purchased_at,
        seller_versions.seller_version_id,
        seller_versions.zip_code_prefix as seller_zip_code_prefix,
        seller_versions.city as seller_city,
        seller_versions.state as seller_state,
        seller_versions.latitude as seller_latitude,
        seller_versions.longitude as seller_longitude,
        seller_versions.is_current as is_seller_version_current
    from order_items
    left join seller_versions
        on
            order_items.seller_id = seller_versions.seller_id
            and order_items.purchased_at >= seller_versions.valid_from
            and order_items.purchased_at < seller_versions.valid_to
)

select * from final
