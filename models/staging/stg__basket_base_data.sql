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

  FROM {{ source('ga4_obfuscated_sample_ecommerce', 'events_*') }} t
  LEFT JOIN UNNEST(items) i
  LEFT JOIN {{ source('customer', 'users') }} u
    ON u.user_pseudo_id = t.user_pseudo_id
  LEFT JOIN {{ ref('stg__purchase_sessions') }} np 
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