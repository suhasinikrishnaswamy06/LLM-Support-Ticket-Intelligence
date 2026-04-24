select
    cast(ticket_id as string) as ticket_id,
    cast(thread_id as string) as thread_id,
    cast(slack_channel as string) as slack_channel,
    cast(customer_name as string) as customer_name,
    cast(created_at as timestamp) as created_at,
    cast(message_text as string) as message_text,
    cast(failure_type as string) as failure_type,
    cast(failure_reason as string) as failure_reason,
    cast(model_name as string) as model_name,
    cast(prompt_version as string) as prompt_version,
    cast(failed_at as timestamp) as failed_at
from {{ source('support_raw', 'ticket_enrichment_failures') }}
