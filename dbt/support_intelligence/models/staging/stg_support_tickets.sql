select
    cast(ticket_id as string) as ticket_id,
    cast(thread_id as string) as thread_id,
    cast(slack_channel as string) as slack_channel,
    cast(customer_name as string) as customer_name,
    cast(created_at as timestamp) as created_at,
    cast(message_text as string) as message_text
from {{ source('support_raw', 'support_tickets') }}
