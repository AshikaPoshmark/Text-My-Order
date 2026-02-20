-- Path in Datagrip : /Users/ashika/DataGripProjects/test/Text My Order/Text My Order AB test.sql

-- Below are the tables calculated code for Text My Order Analysis

---------------------------------------------------- count Overall, D0, D6 Orders based on SMS enabled, sellers not shipped in last 30 days ------------------------------------------------------------------------------


select DATE(ashika_text_my_order.booked_at)                                                                             as booked_date,
CASE
  WHEN dw_users.extended_signup_segments_v3.L17 IN (
    '001', '003', '005', '006', '008', '009', '010', '014', '016', '017',
    '018', '020', '021', '025', '026', '027', '028', '030', '031', '033',
    '034', '037', '039', '041', '042', '044', '046', '048', '052', '053',
    '058', '059', '061', '062', '063', '065', '066', '070', '071', '075',
    '078', '079', '081', '086', '088', '089', '090', '093', '095', '096',
    '097', '098', '101', '103', '105', '106', '107', '109', '110', '114',
    '119', '120', '122', '127'
  ) THEN 'Roll-out'
  ELSE 'Control' END                                                                                                    AS test_group,

  count(distinct ashika_text_my_order.order_id)                                                                         as orders,
  count(distinct case when b.user_id is not null
                            and b.sms_permissions_enabled_updated_at <= ashika_text_my_order.booked_at
                      THEN ashika_text_my_order.order_id end)                                                           as sms_enabled_sellers_orders,

  count(distinct case when ashika_text_my_order.seller_id is null
                      then ashika_text_my_order.order_id end )                                                          as not_shipped_in_last_30_days_sellers_orders,

  count(distinct case when ashika_text_my_order.seller_id is null
                               and b.user_id is not null 
                               and b.sms_permissions_enabled_updated_at <= ashika_text_my_order.booked_at
                      then ashika_text_my_order.order_id end )                                                          as sms_enabled_not_shipped_in_last_30_days_sellers_orders,

  count(distinct case when d0_order_id is not null THEN ashika_text_my_order.order_id END)                              as D0_orders,

  count(distinct case when d0_order_id is not null and ashika_text_my_order.seller_id is null
                        THEN ashika_text_my_order.order_id END)                                                         as D0_sent_orders_for_not_shipped_sellers,

  count(distinct case when d0_order_id is not null and b.user_id is not null
                                and b.sms_permissions_enabled_updated_at <= ashika_text_my_order.booked_at
                      THEN ashika_text_my_order.order_id END)                                                           as D0_orders_sms_enabled,

  count(distinct case when d0_order_id is not null 
                               and ashika_text_my_order.seller_id is null
                               and b.user_id is not null 
                               and b.sms_permissions_enabled_updated_at <= ashika_text_my_order.booked_at
                      THEN ashika_text_my_order.order_id END)                                                           as D0_sent_orders_for_not_shipped_sellers_sms_enabled,

--  count(distinct case when date_diff('day', ashika_text_my_order.booked_at, ashika_text_my_order.shipped_at) <= 5
--                         THEN ashika_text_my_order.order_id END) as D0_D5_shipped_orders,
--
--  count(distinct case when date_diff('day', ashika_text_my_order.booked_at, ashika_text_my_order.shipped_at) <= 5 
    --                          and d0_order_id is not null
--                         THEN ashika_text_my_order.order_id END) as sms_sent_D0_D5_shipped_orders,

  count(distinct case when (date_diff('day', ashika_text_my_order.booked_at, ashika_text_my_order.shipped_at) > 5 
                            or ashika_text_my_order.shipped_at is null)
                           and (date_diff('day', ashika_text_my_order.booked_at, dw_orders.cancelled_on) > 5 
                            or dw_orders.cancelled_on is null)
                           and d6_sms_enabled_users.user_id is not null
                      THEN ashika_text_my_order.order_id END)                                                           as d6_sms_qualities_orders,

  count(distinct case when d6_order_id is not null 
                      THEN ashika_text_my_order.order_id END)                                                           as D6_orders,
    
  count(distinct case when d6_order_id is not null 
                            and d0_order_id is not null 
                      THEN ashika_text_my_order.order_id END)                                                           as D6_sms_sent_orders_who_also_received_D0_sms,
    
  count(distinct case when (date_diff('day', ashika_text_my_order.booked_at, ashika_text_my_order.shipped_at) > 5 
                                or ashika_text_my_order.shipped_at is null)
                            and (date_diff('day', ashika_text_my_order.booked_at, dw_orders.cancelled_on) > 5 
                                or dw_orders.cancelled_on is null)
                            and d6_sms_enabled_users.user_id is not null
                        THEN d6_order_id END)                                                                           as d6_sms_send_to_qualities_orders --,

-- count(distinct case when date_diff('day', ashika_text_my_order.booked_at, ashika_text_my_order.shipped_at) <= 5 and b.user_id is not null
--                              and b.sms_permissions_enabled_updated_at <= ashika_text_my_order.booked_at THEN ashika_text_my_order.order_id END) as sms_enabled_users_D0_D5_shipped_orders,
--
--   count( distinct case when ashika_text_my_order.shipped_at is not null then ashika_text_my_order.order_id end) as shipped_orders


   from analytics_scratch.ashika_text_my_order1 as ashika_text_my_order
       left join analytics.dw_orders on dw_orders.order_id = ashika_text_my_order.order_id
       --left join analytics.dw_users_info on dw_users_info.user_id = ashika_text_my_order.order_seller_id
       left join analytics.dw_users on dw_users.user_id = ashika_text_my_order.order_seller_id
       left join analytics_scratch.ashika_sms_enabled_users as b 
           on b.snapshot_date = ashika_text_my_order.booked_date and b.user_id = ashika_text_my_order.order_seller_id
       left join analytics_scratch.ashika_sms_enabled_users as d6_sms_enabled_users 
           on d6_sms_enabled_users.snapshot_date= date(date_add('day',6,ashika_text_my_order.booked_date))
            and d6_sms_enabled_users.user_id = ashika_text_my_order.order_seller_id
   
group by 1,2
order by 1 desc;



--------------------------------------------------------------  Overall, D0, D6 Orders Sellers count based on SMS enabled, sellers not shipped in last 30 days ------------------------------------------------------------------



select DATE(ashika_text_my_order.booked_at)                                                                             as booked_date,
CASE
  WHEN dw_users.extended_signup_segments_v3.L17 IN (
    '001', '003', '005', '006', '008', '009', '010', '014', '016', '017',
    '018', '020', '021', '025', '026', '027', '028', '030', '031', '033',
    '034', '037', '039', '041', '042', '044', '046', '048', '052', '053',
    '058', '059', '061', '062', '063', '065', '066', '070', '071', '075',
    '078', '079', '081', '086', '088', '089', '090', '093', '095', '096',
    '097', '098', '101', '103', '105', '106', '107', '109', '110', '114',
    '119', '120', '122', '127'
  ) THEN 'Roll-out'
  ELSE 'Control' END                                                                                                    AS test_group,

  count(distinct ashika_text_my_order.order_seller_id)                                                                  as orders,
  count(distinct case when b.user_id is not null
                            and b.sms_permissions_enabled_updated_at <= ashika_text_my_order.booked_at
                        THEN ashika_text_my_order.order_seller_id end)                                                  as sms_enabled_sellers_orders,

  count(distinct case when ashika_text_my_order.seller_id is null
                      then ashika_text_my_order.order_seller_id end )                                                   as not_shipped_in_last_30_days_sellers_orders,

  count(distinct case when ashika_text_my_order.seller_id is null
                            and b.user_id is not null 
                            and b.sms_permissions_enabled_updated_at <= ashika_text_my_order.booked_at
                        then ashika_text_my_order.order_seller_id end )                                                 as sms_enabled_not_shipped_in_last_30_days_sellers_orders,

  count(distinct case when d0_order_id is not null THEN ashika_text_my_order.order_seller_id END)                       as D0_orders,

  count(distinct case when d0_order_id is not null and ashika_text_my_order.seller_id is null
                        THEN ashika_text_my_order.order_seller_id END)                                                  as D0_sent_orders_for_not_shipped_sellers,

  count(distinct case when d0_order_id is not null and b.user_id is not null
                            and b.sms_permissions_enabled_updated_at <= ashika_text_my_order.booked_at
      THEN ashika_text_my_order.order_seller_id END)                                                                    as D0_orders_sms_enabled,

  count(distinct case when d0_order_id is not null and ashika_text_my_order.seller_id is null
                            and b.user_id is not null 
                            and b.sms_permissions_enabled_updated_at <= ashika_text_my_order.booked_at
      THEN ashika_text_my_order.order_seller_id END)                                                                    as D0_sent_orders_for_not_shipped_sellers_sms_enabled,

--   count(distinct case when date_diff('day', ashika_text_my_order.booked_at, ashika_text_my_order.shipped_at) <= 5
--                         THEN ashika_text_my_order.order_seller_id END)                                               as D0_D5_shipped_orders,
--
--   count(distinct case when date_diff('day', ashika_text_my_order.booked_at, ashika_text_my_order.shipped_at) <= 5 
    --                          and d0_order_id is not null
--                         THEN ashika_text_my_order.order_seller_id END)                                               as sms_sent_D0_D5_shipped_orders,

  count(distinct case when (date_diff('day', ashika_text_my_order.booked_at, ashika_text_my_order.shipped_at) > 5 
                                or ashika_text_my_order.shipped_at is null)
                            and (date_diff('day', ashika_text_my_order.booked_at, dw_orders.cancelled_on) > 5 
                                or dw_orders.cancelled_on is null)
                            and d6_sms_enabled_users.user_id is not null
                      THEN ashika_text_my_order.order_seller_id END)                                                    as d6_sms_qualities_orders,

  count(distinct case when d6_order_id is not null THEN ashika_text_my_order.order_seller_id END)                       as D6_orders,
  
  count(distinct case when d6_order_id is not null 
                            and d0_order_id is not null 
                      THEN ashika_text_my_order.order_seller_id END)                                                    as D6_sms_sent_orders_who_also_received_D0_sms,
    
  count(distinct case when (date_diff('day', ashika_text_my_order.booked_at, ashika_text_my_order.shipped_at) > 5 
                                  or ashika_text_my_order.shipped_at is null)
                               and (date_diff('day', ashika_text_my_order.booked_at, dw_orders.cancelled_on) > 5 
                                  or dw_orders.cancelled_on is null)
                               and d6_sms_enabled_users.user_id is not null 
                               and d6_order_id is not null
                      THEN ashika_text_my_order.order_seller_id END)                                                    as d6_sms_send_to_qualities_orders --,

-- count(distinct case when date_diff('day', ashika_text_my_order.booked_at, ashika_text_my_order.shipped_at) <= 5 
--                            and b.user_id is not null
--                            and b.sms_permissions_enabled_updated_at <= ashika_text_my_order.booked_at 
--                     THEN ashika_text_my_order.order_id END)                                                          as sms_enabled_users_D0_D5_shipped_orders,
--
--   count( distinct case when ashika_text_my_order.shipped_at is not null 
--                        then ashika_text_my_order.order_id end)                                                       as shipped_orders

   from analytics_scratch.ashika_text_my_order1 as ashika_text_my_order
       left join analytics.dw_orders on dw_orders.order_id = ashika_text_my_order.order_id
       --left join analytics.dw_users_info on dw_users_info.user_id = ashika_text_my_order.order_seller_id
       left join analytics.dw_users on dw_users.user_id = ashika_text_my_order.order_seller_id
       left join analytics_scratch.ashika_sms_enabled_users as b 
           on b.snapshot_date = ashika_text_my_order.booked_date and b.user_id = ashika_text_my_order.order_seller_id
       left join analytics_scratch.ashika_sms_enabled_users as d6_sms_enabled_users 
           on d6_sms_enabled_users.snapshot_date= date(date_add('day',6,ashika_text_my_order.booked_date))
            and d6_sms_enabled_users.user_id = ashika_text_my_order.order_seller_id
   
group by 1,2
order by 1 desc,2;




--------------------------------------------------  D7 shipped orders % calculation for sms enabled orders, sellers not shipped in last 30 days  -------------------------------------------------------





select booked_date,
CASE
  WHEN dw_users.extended_signup_segments_v3.L17 IN (
    '001', '003', '005', '006', '008', '009', '010', '014', '016', '017',
    '018', '020', '021', '025', '026', '027', '028', '030', '031', '033',
    '034', '037', '039', '041', '042', '044', '046', '048', '052', '053',
    '058', '059', '061', '062', '063', '065', '066', '070', '071', '075',
    '078', '079', '081', '086', '088', '089', '090', '093', '095', '096',
    '097', '098', '101', '103', '105', '106', '107', '109', '110', '114',
    '119', '120', '122', '127'
  ) THEN 'Roll-out'
  ELSE 'Control' END                                                                                                    AS test_group,

  count(distinct ashika_text_my_order.order_id)                                                                         as orders,
  count(distinct ashika_text_my_order.order_seller_id)                                                                  as sellers,

  count(distinct case when b.user_id is not null
                            and date(b.sms_permissions_enabled_updated_at) <= date(ashika_text_my_order.booked_at)
                        THEN ashika_text_my_order.order_id end)                                                         as sms_enabled_sellers_orders,

  count(distinct case when b.user_id is not null
                            and date(b.sms_permissions_enabled_updated_at) <= date(ashika_text_my_order.booked_at)
                        THEN ashika_text_my_order.order_seller_id end)                                                  as sms_enabled_orders_sellers,

  count(distinct case when ashika_text_my_order.seller_id is null
                      then ashika_text_my_order.order_id end )                                                          as not_shipped_in_last_30_days_sellers_orders,
    
  count(distinct case when ashika_text_my_order.seller_id is null
                      then ashika_text_my_order.order_seller_id end )                                                   as not_shipped_in_last_30_days_sellers_orders_sellers,

  count(distinct case when ashika_text_my_order.seller_id is null
                               and b.user_id is not null 
                               and date(b.sms_permissions_enabled_updated_at) <= date(ashika_text_my_order.booked_at)
                        then ashika_text_my_order.order_id end )                                                        as sms_enabled_not_shipped_in_last_30_days_sellers_orders,

  count(distinct case when ashika_text_my_order.seller_id is null
                               and b.user_id is not null  
                               and date(b.sms_permissions_enabled_updated_at) <= date(ashika_text_my_order.booked_at)
                        then ashika_text_my_order.order_seller_id end )                                                 as sms_enabled_not_shipped_in_last_30_days_sellers_orders_sellers,

--   count(distinct case when d0_order_id is not null THEN ashika_text_my_order.order_id END)                           as D0_orders,
--
--   count(distinct case when d0_order_id is not null and ashika_text_my_order.seller_id is null
--                         THEN ashika_text_my_order.order_id END)          as D0_sent_orders_for_not_shipped_sellers,
--
--   count(distinct case when d0_order_id is not null and b.user_id is not null
--                                 and b.sms_permissions_enabled_updated_at <= ashika_text_my_order.booked_at
--       THEN ashika_text_my_order.order_id END) as D0_orders_sms_enabled,
--
--   count(distinct case when d0_order_id is not null and ashika_text_my_order.seller_id is null
--                                and b.user_id is not null and date(b.sms_permissions_enabled_updated_at) <= date(ashika_text_my_order.booked_at)
--       THEN ashika_text_my_order.order_seller_id END) as D0_sent_orders_for_not_shipped_sellers_sms_enabled,
--
--       count(distinct case when d0_order_id is not null and ashika_text_my_order.seller_id is null
--                                and b.user_id is not null and date(b.sms_permissions_enabled_updated_at) = date(ashika_text_my_order.booked_at)
--       THEN ashika_text_my_order.order_seller_id END) as D0_sent_orders_for_not_shipped_sellers_sms_enabled,

  count(distinct case when date_diff('day', ashika_text_my_order.booked_at, dw_orders.shipped_at) <= 7
                        THEN ashika_text_my_order.order_id END)                                                         as D7_shipped_orders,

  count(distinct case when date_diff('day', ashika_text_my_order.booked_at, dw_orders.shipped_at) <= 7
                        THEN ashika_text_my_order.order_seller_id END)                                                  as D7_shipped_sellers,

  count(distinct case when (date_diff('day', ashika_text_my_order.booked_at, dw_orders.shipped_at) <= 7) 
                                and b.user_id is not null
                                and date(b.sms_permissions_enabled_updated_at) <= date(ashika_text_my_order.booked_at)
                        THEN ashika_text_my_order.order_id END)                                                         as D7_shipped_orders_for_sms_enabled,

  count(distinct case when (date_diff('day', ashika_text_my_order.booked_at, dw_orders.shipped_at) <= 7) 
                                and b.user_id is not null
                                and date(b.sms_permissions_enabled_updated_at) <= date(ashika_text_my_order.booked_at)
                        THEN ashika_text_my_order.order_seller_id END)                                                  as D7_shipped_sellers_for_sms_enabled,

  count(distinct case when (date_diff('day', ashika_text_my_order.booked_at, dw_orders.shipped_at) <= 7) 
                                and b.user_id is not null
                                and date(b.sms_permissions_enabled_updated_at) <= date(ashika_text_my_order.booked_at) 
                                and  ashika_text_my_order.seller_id is null
                        THEN ashika_text_my_order.order_id END)                                                         as D7_shipped_orders_for_sms_enabled_and_not_shipped,

  count(distinct case when (date_diff('day', ashika_text_my_order.booked_at, dw_orders.shipped_at) <= 7) 
                                and b.user_id is not null
                                and date(b.sms_permissions_enabled_updated_at) <= date(ashika_text_my_order.booked_at) 
                                and  ashika_text_my_order.seller_id is null
                        THEN ashika_text_my_order.order_seller_id END)                                                  as D7_shipped_sellers_for_sms_enabled_and_not_shipped,

  count(distinct case when (date_diff('day', ashika_text_my_order.booked_at, dw_orders.shipped_at) <= 7) 
                               and ashika_text_my_order.seller_id is null
                        THEN ashika_text_my_order.order_id END)                                                         as D7_shipped_orders_for_not_shipped_sellers_last_30_days,

  count(distinct case when (date_diff('day', ashika_text_my_order.booked_at, dw_orders.shipped_at) <= 7) 
                               and ashika_text_my_order.seller_id is null
                        THEN ashika_text_my_order.order_seller_id END)                                                  as D7_shipped_sellers_for_not_shipped_sellers_last_30_days

   from analytics_scratch.ashika_text_my_order1 as ashika_text_my_order
       left join analytics.dw_orders on dw_orders.order_id = ashika_text_my_order.order_id
       --left join an
       -- alytics.dw_users_info on dw_users_info.user_id = ashika_text_my_order.order_seller_id
       left join analytics.dw_users on dw_users.user_id = ashika_text_my_order.order_seller_id
       left join analytics_scratch.ashika_sms_enabled_users as b 
           on b.snapshot_date = ashika_text_my_order.booked_date and b.user_id = ashika_text_my_order.order_seller_id
       left join analytics_scratch.ashika_sms_enabled_users as d6_sms_enabled_users 
           on d6_sms_enabled_users.snapshot_date= date(date_add('day',6,ashika_text_my_order.booked_date))
                and d6_sms_enabled_users.user_id = ashika_text_my_order.order_seller_id
       left join analytics_scratch.ashika_sms_enrolled_users as sms_enrolled_users 
           on ashika_text_my_order.order_seller_id = sms_enrolled_users.user_id

   where date(booked_date) >= '2025-12-08'
group by 1,2
order by 1 desc;





