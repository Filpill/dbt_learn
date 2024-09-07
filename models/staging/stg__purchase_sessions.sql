/*Retrieving sessions that did include a "purchase" event*/
 SELECT DISTINCT
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key ='ga_session_id') AS session_id,
    1 AS session_purchase_flag
FROM {{ source('ga4_obfuscated_sample_ecommerce', 'events_*') }} t
WHERE 
    _TABLE_SUFFIX BETWEEN '{{ var('start_date') }}' AND '{{ var('end_date') }}'
    AND event_name = 'purchase'