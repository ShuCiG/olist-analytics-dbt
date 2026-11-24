-- Every order has an exchange rate: the rates run from before the first purchase to the latest
-- published day. A gap means the feed was down for a stretch or the first load stopped short:
-- the USD columns of fct_order_fx are silently null for those orders.
select
    order_id,
    purchase_date
from {{ ref('fct_order_fx') }}
where brl_per_usd is null
