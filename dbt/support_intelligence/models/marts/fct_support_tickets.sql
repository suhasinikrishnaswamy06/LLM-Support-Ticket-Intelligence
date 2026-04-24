select
    ticket_id,
    thread_id,
    slack_channel,
    customer_name,
    created_at,
    issue_category,
    sentiment,
    urgency,
    product_area,
    summary,
    confidence,
    model_name,
    prompt_version,
    enrichment_method,
    processed_at
from {{ ref('stg_ticket_enrichments') }}
