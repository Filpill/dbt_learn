WITH 
/*Retrieving sessions that did include a "purchase" event*/
cte_purchase_sessions AS (
  SELECT DISTINCT
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key ='ga_session_id') AS session_id,
    1 AS session_purchase_flag
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` t
  WHERE 
    _TABLE_SUFFIX BETWEEN '{{ var('start_date') }}' AND '{{ var('end_date') }}'
    AND event_name = 'purchase'
),

/* Creating driving base dataset
        Maintaining event level grain
        Flag sessions with "purchase" event
        Filtering on events which are directly pulling item/basket data  */
cte_base_data AS (

  SELECT
    UNIX_MICROS(CURRENT_TIMESTAMP()) AS etl_check_ts,
    t.event_date,
    t.user_pseudo_id,
    u.email,
    COALESCE(np.session_purchase_flag,0) AS session_purchase_flag, /*identifies if session contain "purchase" event*/
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key ='ga_session_id') AS session_id,
    t.event_timestamp AS unix_event_ts,
    TIMESTAMP_MICROS(t.event_timestamp) AS event_timestamp,
    t.event_name,

    CASE
      WHEN t.event_name IN (
         {{ expand_list(var('basket_event_list')) }}
    )
      THEN 1 ELSE 0
    END AS basket_related_event,

    ARRAY_TO_STRING(
        ARRAY_AGG(i.item_id ORDER BY i.item_id ASC),','
    ) AS basket_list,

    MD5(
       ARRAY_TO_STRING(
        ARRAY_AGG(i.item_id ORDER BY i.item_id ASC),','
      )
    ) AS basket_hash

  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` t
  LEFT JOIN UNNEST(items) i
  LEFT JOIN `data-eng11.dbt_sample_ecommerce.users` u
    ON u.user_pseudo_id = t.user_pseudo_id
  LEFT JOIN cte_purchase_sessions np 
    ON (SELECT value.int_value FROM UNNEST(event_params) WHERE key ='ga_session_id') = np.session_id

  WHERE _TABLE_SUFFIX BETWEEN '{{ var('start_date') }}' AND '{{ var('end_date') }}'
    AND t.event_name IN (
        {{ expand_list(var('basket_event_list')) }}
) /*Exclusively Filtering on Events with Basket Data*/

  GROUP BY 
    session_id,
    np.session_purchase_flag,
    u.email,
    t.user_pseudo_id,
    t.event_date,
    t.event_timestamp,
    t.event_previous_timestamp,
    t.event_name

  ORDER BY 
    user_pseudo_id ASC, 
    event_timestamp ASC
),

/*Partition events into sessions and ordering the events by the timestamp*/
cte_event_order AS (

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
  FROM cte_base_data t
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
ORDER BY session_id, event_timestamp DESC