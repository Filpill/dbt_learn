WITH cte_event_order AS (

  SELECT 
    t.etl_check_ts,
    t.event_date,
    t.user_pseudo_id,
    t.email,
    t.session_purchase_flag,
    t.session_id,
    t.event_name,
    t.unix_event_ts,
    ROW_NUMBER() OVER(PARTITION BY t.session_id ORDER BY t.unix_event_ts DESC) AS session_event_order, /*Ordered events from newest to oldest*/
    t.event_timestamp,
    t.basket_list,
  FROM {{ ref('stg__basket_base_data') }} t
),

/*  Calculate the time differentials for abandonment criteria
      event_to_event_delta - Represents the amount of time spent between each basket event (between each session)
      session_start_delta - This is the amount of time elapsed since the first basket event was initatied */
cte_time_delta AS (

  SELECT 
    t.* except(basket_list,session_purchase_flag),

        CASE 
          WHEN session_event_order = 1 THEN (t.etl_check_ts - t.unix_event_ts)/1000000
          WHEN session_event_order > 1 THEN (LAG(t.unix_event_ts) OVER(PARTITION BY t.session_id ORDER BY t.unix_event_ts DESC) - t.unix_event_ts)/1000000 
        END AS event_to_event_delta,

        CASE 
          WHEN session_event_order = 1 THEN (t.etl_check_ts - t.unix_event_ts)/1000000
          WHEN session_event_order > 1 THEN (t.unix_event_ts - MIN(t.unix_event_ts) OVER(PARTITION BY t.session_id ORDER BY t.unix_event_ts ASC))/1000000 
        END AS session_start_delta,

    t.basket_list,
    t.session_purchase_flag

  FROM cte_event_order t
  
),

/*Generate Flags which signal basket abandonment*/
cte_flag_calc AS (

  SELECT 
    t.*,
    CASE WHEN t.event_to_event_delta >= {{ var('event_level_abandon_threshold') }} THEN 1 ELSE 0 END AS event_abandon_flag,
    CASE WHEN t.session_start_delta >= {{ var('session_level_abandon_threshold') }} THEN 1 ELSE 0 END AS session_abandon_flag
  FROM cte_time_delta t

)

SELECT * FROM cte_flag_calc