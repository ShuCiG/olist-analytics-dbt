-- Grain: one row per shipment = order × seller: each seller ships its items of
-- an order from its own location. Seller as of the purchase (SCD2 history),
-- customer at the order's zip prefix; both located at their zip centroid, so the
-- distance is centroid to centroid (null when either prefix has no coordinates).
-- Olist records one delivery date per order: all shipments of an order share it.
with seller_items as (
    select * from {{ ref('fct_order_items_seller_as_of') }}
),

orders as (
    select * from {{ ref('int_orders_joined_to_customers') }}
),

delivery as (
    select * from {{ ref('int_orders_delivery_metrics') }}
),

zip_codes as (
    select * from {{ ref('int_zip_codes_located') }}
),

shipments as (
    -- The seller version is fixed by the purchase time, so it is the same for
    -- every item of a seller in an order: max() just picks it
    select
        order_id,
        seller_id,
        count(*) as items_count,
        max(seller_state) as seller_state,
        max(seller_latitude) as seller_latitude,
        max(seller_longitude) as seller_longitude
    from seller_items
    group by order_id, seller_id
),

measured as (
    select
        {{ dbt_utils.generate_surrogate_key(['shipments.order_id', 'shipments.seller_id']) }} as shipment_sk,
        shipments.order_id,
        shipments.seller_id,
        cast(orders.purchased_at as date) as purchase_date,
        shipments.items_count,
        shipments.seller_state,
        orders.state as customer_state,
        {{ to_float(haversine_km(
            'shipments.seller_latitude', 'shipments.seller_longitude',
            'customer_zip.latitude', 'customer_zip.longitude'
        )) }} as distance_km,
        delivery.is_delivered,
        delivery.days_to_deliver,
        delivery.is_delivered_late
    from shipments
    inner join orders
        on shipments.order_id = orders.order_id
    inner join delivery
        on shipments.order_id = delivery.order_id
    left join zip_codes as customer_zip
        on orders.zip_code_prefix = customer_zip.zip_code_prefix
),

final as (
    select
        shipment_sk,
        order_id,
        seller_id,
        purchase_date,
        seller_state,
        customer_state,
        seller_state = customer_state as is_same_state,
        distance_km,
        case
            when distance_km is null then 'unknown'
            when distance_km < 100 then '0-100 km'
            when distance_km < 500 then '100-500 km'
            when distance_km < 1000 then '500-1000 km'
            when distance_km < 2000 then '1000-2000 km'
            else '2000+ km'
        end as distance_band,
        items_count,
        is_delivered,
        days_to_deliver,
        is_delivered_late
    from measured
)

select * from final
