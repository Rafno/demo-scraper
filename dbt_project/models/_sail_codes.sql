{# WITH source AS (
    SELECT * 
    FROM 
    {{ source('public', 'store_sail_codes_file') }}
)

select
    *,
    left(sail_code, 2) as ship_code
from source #}