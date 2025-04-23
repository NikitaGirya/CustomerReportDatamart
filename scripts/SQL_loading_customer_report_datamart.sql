-- DML витрины dwh.customer_report_datamart

with max_mart_load_dttm as (
    -- рассчитываем максимальный таймстемп загрузки витрины (для инита - 1900-01-01)
    select coalesce(max(load_dttm), '1900-01-01'::timestamp) as max_load_dttm
    from dwh.load_dates_customer_report_datamart
)
, dwh_delta as (
    -- формируем дельту изменений (т.е. обновленные/вставленные после даты последнего обновления витрины данные из источников)
    select
        dcs.customer_id
        , dcs.customer_name
        , dcs.customer_address
        , dcs.customer_birthday
        , dcs.customer_email
        , dcf.craftsman_id
        , fo.order_id
        , dp.product_id
        , dp.product_price
        , dp.product_type
        , fo.order_status
        , crd.customer_id as exist_customer_id
        , dcf.load_dttm as craftsman_load_dttm
        , dcs.load_dttm as customer_load_dttm
        , dp.load_dttm as product_load_dttm
        , (fo.order_completion_date - fo.order_created_date) as diff_order_date
        , to_char(fo.order_created_date, 'yyyy-mm') as report_period
    from dwh.f_order as fo
    inner join dwh.d_craftsman as dcf
        on fo.craftsman_id = dcf.craftsman_id
    inner join dwh.d_customer as dcs
        on fo.customer_id = dcs.customer_id
    inner join dwh.d_product as dp
        on fo.product_id = dp.product_id
    left join dwh.customer_report_datamart as crd
        on dcs.customer_id = crd.customer_id
    cross join max_mart_load_dttm as mmld
    where
        (fo.load_dttm > mmld.max_load_dttm)
        or (dcf.load_dttm > mmld.max_load_dttm)
        or (dcs.load_dttm > mmld.max_load_dttm)
        or (dp.load_dttm > mmld.max_load_dttm)
)
, exist_customer_id as (
    -- создаем выборку клиентов, по которым были изменения в DWH для обновления данных в витрине
    select exist_customer_id as customer_id
    from dwh_delta
    where exist_customer_id is not null
)
, top_product_category as (
    -- определяем самую популярную категорию товара у клиента
    select
        customer_id as customer_id_for_top_product_category
        , product_type as top_product_category
        , row_number()
            over (
                partition by customer_id
                order by count_product desc
            )
        as rn
    from
        (
            select
                customer_id
                , product_type
                , count(product_id) as count_product
            from dwh_delta
            group by
                customer_id
                , product_type
        ) as sq
)
, top_craftsman_id as (
    -- определяем самого популярного мастера у клиента
    select
        customer_id as customer_id_for_top_craftsman_id
        , craftsman_id as top_craftsman_id
        , row_number()
            over (
                partition by customer_id
                order by count_order desc
            )
        as rn
    from
        (
            select
                customer_id
                , craftsman_id
                , count(order_id) as count_order
            from dwh_delta
            group by
                customer_id
                , craftsman_id
        ) as sq
)
, dwh_delta_insert_calc as (
    -- производим расчёт по новым данным, далее они просто вставятся в витрину
    select
        customer_id
        , customer_name
        , customer_address
        , customer_birthday
        , customer_email
        , customer_paid_money
        , platform_received_money
        , count_order
        , avg_price_order
        , median_time_order_completed
        , top_product_category
        , top_craftsman_id
        , count_order_created
        , count_order_in_progress
        , count_order_delivery
        , count_order_done
        , count_order_not_done
        , report_period
    from
        (
            select
                customer_id
                , customer_name
                , customer_address
                , customer_birthday
                , customer_email
                , report_period
                , sum(product_price) as customer_paid_money
                , (sum(product_price) * 0.1) as platform_received_money
                , count(order_id) as count_order
                , avg(product_price) as avg_price_order
                , percentile_cont(0.5) within group (
                    order by diff_order_date
                ) as median_time_order_completed
                , sum(
                    case when order_status = 'created' then 1 else 0 end
                ) as count_order_created
                , sum(
                    case when order_status = 'in progress' then 1 else 0 end
                ) as count_order_in_progress
                , sum(
                    case when order_status = 'delivery' then 1 else 0 end
                ) as count_order_delivery
                , sum(
                    case when order_status = 'done' then 1 else 0 end
                ) as count_order_done
                , sum(
                    case when order_status != 'done' then 1 else 0 end
                ) as count_order_not_done
            from dwh_delta
            where exist_customer_id is null
            group by
                customer_id
                , customer_name
                , customer_address
                , customer_birthday
                , customer_email
                , report_period
        ) as calc
    inner join top_product_category as tpc
        on
            calc.customer_id = tpc.customer_id_for_top_product_category
            and tpc.rn = 1
    inner join top_craftsman_id as tci
        on
            calc.customer_id = tci.customer_id_for_top_craftsman_id
            and tci.rn = 1
)
, dwh_delta_update_calc as (
    -- производим перерасчёт для существующих записей, далее они обновятся в витрине
    select
        customer_id
        , customer_name
        , customer_address
        , customer_birthday
        , customer_email
        , customer_paid_money
        , platform_received_money
        , count_order
        , avg_price_order
        , median_time_order_completed
        , top_product_category
        , top_craftsman_id
        , count_order_created
        , count_order_in_progress
        , count_order_delivery
        , count_order_done
        , count_order_not_done
        , report_period
    from
        (
            select
                customer_id
                , customer_name
                , customer_address
                , customer_birthday
                , customer_email
                , report_period
                , sum(product_price) as customer_paid_money
                , (sum(product_price) * 0.1) as platform_received_money
                , count(order_id) as count_order
                , avg(product_price) as avg_price_order
                , percentile_cont(0.5) within group (
                    order by diff_order_date
                ) as median_time_order_completed
                , sum(
                    case when order_status = 'created' then 1 else 0 end
                ) as count_order_created
                , sum(
                    case when order_status = 'in progress' then 1 else 0 end
                ) as count_order_in_progress
                , sum(
                    case when order_status = 'delivery' then 1 else 0 end
                ) as count_order_delivery
                , sum(
                    case when order_status = 'done' then 1 else 0 end
                ) as count_order_done
                , sum(
                    case when order_status != 'done' then 1 else 0 end
                ) as count_order_not_done
            from
                (
                    select
                        dcs.customer_id
                        , dcs.customer_name
                        , dcs.customer_address
                        , dcs.customer_birthday
                        , dcs.customer_email
                        , fo.order_id
                        , dp.product_id
                        , dp.product_price
                        , dp.product_type
                        , fo.order_status
                        , (
                            fo.order_completion_date - fo.order_created_date
                        ) as diff_order_date
                        , to_char(
                            fo.order_created_date, 'yyyy-mm'
                        ) as report_period
                    from dwh.f_order as fo
                    inner join dwh.d_craftsman as dcf
                        on fo.craftsman_id = dcf.craftsman_id
                    inner join dwh.d_customer as dcs
                        on fo.customer_id = dcs.customer_id
                    inner join dwh.d_product as dp
                        on fo.product_id = dp.product_id
                    inner join exist_customer_id as eci
                        on fo.customer_id = eci.customer_id
                ) as exists_data
            group by
                customer_id
                , customer_name
                , customer_address
                , customer_birthday
                , customer_email
                , report_period
        ) as calc
    inner join top_product_category as tpc
        on
            calc.customer_id = tpc.customer_id_for_top_product_category
            and tpc.rn = 1
    inner join top_craftsman_id as tci
        on
            calc.customer_id = tci.customer_id_for_top_craftsman_id
            and tci.rn = 1
)
, insert_delta as (
    -- выполняем insert новых расчитанных данных в витрину
    insert into dwh.customer_report_datamart
    (
        customer_id
        , customer_name
        , customer_address
        , customer_birthday
        , customer_email
        , customer_paid_money
        , platform_received_money
        , count_order
        , avg_price_order
        , median_time_order_completed
        , top_product_category
        , top_craftsman_id
        , count_order_created
        , count_order_in_progress
        , count_order_delivery
        , count_order_done
        , count_order_not_done
        , report_period
    )
    select
        customer_id
        , customer_name
        , customer_address
        , customer_birthday
        , customer_email
        , customer_paid_money
        , platform_received_money
        , count_order
        , avg_price_order
        , median_time_order_completed
        , top_product_category
        , top_craftsman_id
        , count_order_created
        , count_order_in_progress
        , count_order_delivery
        , count_order_done
        , count_order_not_done
        , report_period
    from dwh_delta_insert_calc
)
, update_delta as (
    -- выполняем update расчитанных данных в витрине
    update dwh.customer_report_datamart set
        customer_id = updates.customer_id
        , customer_name = updates.customer_name
        , customer_address = updates.customer_address
        , customer_birthday = updates.customer_birthday
        , customer_email = updates.customer_email
        , customer_paid_money = updates.customer_paid_money
        , platform_received_money = updates.platform_received_money
        , count_order = updates.count_order
        , avg_price_order = updates.avg_price_order
        , median_time_order_completed = updates.median_time_order_completed
        , top_product_category = updates.top_product_category
        , top_craftsman_id = updates.top_craftsman_id
        , count_order_created = updates.count_order_created
        , count_order_in_progress = updates.count_order_in_progress
        , count_order_delivery = updates.count_order_delivery
        , count_order_done = updates.count_order_done
        , count_order_not_done = updates.count_order_not_done
        , report_period = updates.report_period
    from
        (
            select
                customer_id
                , customer_name
                , customer_address
                , customer_birthday
                , customer_email
                , customer_paid_money
                , platform_received_money
                , count_order
                , avg_price_order
                , median_time_order_completed
                , top_product_category
                , top_craftsman_id
                , count_order_created
                , count_order_in_progress
                , count_order_delivery
                , count_order_done
                , count_order_not_done
                , report_period
            from dwh_delta_update_calc
        ) as updates
    where dwh.customer_report_datamart.customer_id = updates.customer_id
)
, insert_load_date as (
    -- вставляем таймстемп текущей загрузки в таблицу загрузок
    insert into dwh.load_dates_customer_report_datamart (load_dttm)
    select
        greatest(
            coalesce(max(craftsman_load_dttm), now())
            , coalesce(max(customer_load_dttm), now())
            , coalesce(max(product_load_dttm), now())
        )
    from dwh_delta
)
-- инициализируем запрос
select 'incremental load success' as load_status;
