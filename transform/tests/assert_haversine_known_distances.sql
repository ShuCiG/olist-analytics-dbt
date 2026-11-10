-- haversine_km against distances computed independently (Python, same formula
-- and radius): 0.5 km tolerance, so only a wrong formula fails, not float noise;
-- a missing coordinate must give null
with points as (
    select
        'sao_paulo-rio' as pair,
        -23.5505 as lat1,
        -46.6333 as lng1,
        -22.9068 as lat2,
        -43.1729 as lng2,
        360.75 as expected_km
    union all
    select
        'sao_paulo-manaus' as pair,
        -23.5505 as lat1,
        -46.6333 as lng1,
        -3.1190 as lat2,
        -60.0217 as lng2,
        2689.47 as expected_km
    union all
    select
        'porto_alegre-manaus' as pair,
        -30.0346 as lat1,
        -51.2177 as lng1,
        -3.1190 as lat2,
        -60.0217 as lng2,
        3133.20 as expected_km
    union all
    select
        'same_point' as pair,
        -22.9068 as lat1,
        -43.1729 as lng1,
        -22.9068 as lat2,
        -43.1729 as lng2,
        0 as expected_km
    union all
    -- a missing coordinate gives no distance (expected null)
    select
        'missing_point' as pair,
        null as lat1,
        null as lng1,
        -22.9068 as lat2,
        -43.1729 as lng2,
        null as expected_km
),

measured as (
    select
        pair,
        expected_km,
        {{ haversine_km('lat1', 'lng1', 'lat2', 'lng2') }} as distance_km
    from points
)

select *
from measured
where
    (expected_km is null and distance_km is not null)
    or (expected_km is not null and (distance_km is null or abs(distance_km - expected_km) > 0.5))
