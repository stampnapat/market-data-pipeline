-- Asset dimension: one row per tradable thing we track, from a version-controlled seed.
select
    asset_id,
    asset_code,
    asset_name,
    asset_class,
    quote_currency,
    price_unit,
    source_system
from {{ ref('asset_reference') }}
