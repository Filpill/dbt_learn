WITH 
cte_purchase_sessions AS (
  SELECT * 
  FROM {{ ref('stg__purchase_sessions') }}
),

cte_base_data AS (
  SELECT * 
  FROM {{ ref('stg__basket_base_data') }}
),

/*Partition events into sessions and ordering the events by the timestamp*/
cte_abandon_flag AS (
    SELECT *
    FROM {{ ref('stg__abandon_flag_tf') }}
)

SELECT * FROM cte_abandon_flag 
ORDER BY session_id, event_timestamp DESC