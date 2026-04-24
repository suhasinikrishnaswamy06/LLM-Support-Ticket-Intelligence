select
    date(created_at) as ticket_date,
    product_area,
    issue_category,
    urgency,
    sentiment,
    count(*) as ticket_count,
    avg(confidence) as avg_model_confidence
from {{ ref('fct_support_tickets') }}
group by 1, 2, 3, 4, 5
