{#
    Great-circle distance in km between two points in degrees (haversine, mean
    Earth radius 6,371.0088 km). Built only from functions every engine has:
    BigQuery has no radians() or pi(), so degrees are scaled by acos(-1) / 180.
    The clamp to 1 keeps rounding from pushing asin() past its domain (NaN) for
    near-antipodal points. It is a case, not least(1, x): DuckDB's least() skips
    nulls (least(1, null) = 1, a half-circumference distance), the other engines
    return null. Null when any coordinate is null.
#}
{% macro haversine_km(lat1, lng1, lat2, lng2) -%}
    {%- set to_rad = '(acos(-1) / 180)' -%}
    {%- set h -%}
        power(sin(({{ lat2 }} - {{ lat1 }}) * {{ to_rad }} / 2), 2)
        + cos({{ lat1 }} * {{ to_rad }}) * cos({{ lat2 }} * {{ to_rad }})
        * power(sin(({{ lng2 }} - {{ lng1 }}) * {{ to_rad }} / 2), 2)
    {%- endset -%}
    2 * 6371.0088 * asin(sqrt(case when {{ h }} > 1 then 1 else {{ h }} end))
{%- endmacro %}
