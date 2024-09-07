SELECT 
    session_id,
FROM {{ ref('stg__abandon_flag_tf') }}
WHERE NOT
    event_to_event_delta >= 0
AND session_start_delta >= 0