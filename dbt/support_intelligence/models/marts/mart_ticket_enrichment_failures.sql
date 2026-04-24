select
    date(failed_at) as failure_date,
    failure_type,
    model_name,
    prompt_version,
    slack_channel,
    count(*) as failed_ticket_count
from {{ ref('stg_ticket_enrichment_failures') }}
group by 1, 2, 3, 4, 5
