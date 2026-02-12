select
    state,
    brewery_type,
    count(*) as breweries_count
from {{ source('silver', 'openbrewery') }}
group by
    state,
    brewery_type
