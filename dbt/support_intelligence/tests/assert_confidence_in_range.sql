select *
from {{ ref('fct_support_tickets') }}
where confidence < 0 or confidence > 1
