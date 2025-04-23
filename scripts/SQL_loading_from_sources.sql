-- DML загрузки данных из источников в DWH

-- создание временной таблицы tmp_sources с объединенными данными из источников
drop table if exists tmp_sources;
create temp table tmp_sources as
-- source 1
select
    order_id
    , order_created_date
    , order_completion_date
    , order_status
    , craftsman_id
    , craftsman_name
    , craftsman_address
    , craftsman_birthday
    , craftsman_email
    , product_id
    , product_name
    , product_description
    , product_type
    , product_price
    , customer_id
    , customer_name
    , customer_address
    , customer_birthday
    , customer_email
from source1.craft_market_wide
union
-- source 2
select
    cmoc.order_id
    , cmoc.order_created_date
    , cmoc.order_completion_date
    , cmoc.order_status
    , cmmp.craftsman_id
    , cmmp.craftsman_name
    , cmmp.craftsman_address
    , cmmp.craftsman_birthday
    , cmmp.craftsman_email
    , cmmp.product_id
    , cmmp.product_name
    , cmmp.product_description
    , cmmp.product_type
    , cmmp.product_price
    , cmoc.customer_id
    , cmoc.customer_name
    , cmoc.customer_address
    , cmoc.customer_birthday
    , cmoc.customer_email
from source2.craft_market_masters_products as cmmp
inner join source2.craft_market_orders_customers as cmoc
    on
        cmmp.product_id = cmoc.product_id
        and cmmp.craftsman_id = cmoc.craftsman_id
union
-- source 3
select
    cmod.order_id
    , cmod.order_created_date
    , cmod.order_completion_date
    , cmod.order_status
    , cmcf.craftsman_id
    , cmcf.craftsman_name
    , cmcf.craftsman_address
    , cmcf.craftsman_birthday
    , cmcf.craftsman_email
    , cmod.product_id
    , cmod.product_name
    , cmod.product_description
    , cmod.product_type
    , cmod.product_price
    , cmcs.customer_id
    , cmcs.customer_name
    , cmcs.customer_address
    , cmcs.customer_birthday
    , cmcs.customer_email
from source3.craft_market_orders as cmod
inner join source3.craft_market_craftsmans as cmcf
    on cmod.craftsman_id = cmcf.craftsman_id
inner join source3.craft_market_customers as cmcs
    on cmod.customer_id = cmcs.customer_id
union
-- external source
select
    cpo.order_id
    , cpo.order_created_date
    , cpo.order_completion_date
    , cpo.order_status
    , cpo.craftsman_id
    , cpo.craftsman_name
    , cpo.craftsman_address
    , cpo.craftsman_birthday
    , cpo.craftsman_email
    , cpo.product_id
    , cpo.product_name
    , cpo.product_description
    , cpo.product_type
    , cpo.product_price
    , cs.customer_id
    , cs.customer_name
    , cs.customer_address
    , cs.customer_birthday
    , cs.customer_email
from external_source.craft_products_orders as cpo
inner join external_source.customers as cs
    on cpo.customer_id = cs.customer_id;

-- обновление существующих записей и добавление новых в витрину dwh.d_craftsmans
merge into dwh.d_craftsman as dcf
using
    (
        select distinct
            craftsman_name
            , craftsman_address
            , craftsman_birthday
            , craftsman_email
        from tmp_sources
    ) as tmp
    on
        dcf.craftsman_name = tmp.craftsman_name
        and dcf.craftsman_email = tmp.craftsman_email
when matched then
    update set
        craftsman_address = tmp.craftsman_address
        , craftsman_birthday = tmp.craftsman_birthday
        , load_dttm = current_timestamp
when not matched then
    insert
        (
            craftsman_name
            , craftsman_address
            , craftsman_birthday
            , craftsman_email
            , load_dttm
        )
    values (tmp.craftsman_name, tmp.craftsman_address, tmp.craftsman_birthday, tmp.craftsman_email, current_timestamp);

-- обновление существующих записей и добавление новых в витрину dwh.d_products
merge into dwh.d_product as dp
using
    (
        select distinct
            product_name
            , product_description
            , product_type
            , product_price
        from tmp_sources
    ) as tmp
    on
        dp.product_name = tmp.product_name
        and dp.product_description = tmp.product_description
        and dp.product_price = tmp.product_price
when matched then
    update set
        product_type = tmp.product_type
        , load_dttm = current_timestamp
when not matched then
    insert
        (
            product_name
            , product_description
            , product_type
            , product_price
            , load_dttm
        )
    values (tmp.product_name, tmp.product_description, tmp.product_type, tmp.product_price, current_timestamp);

-- обновление существующих записей и добавление новых в витрину dwh.d_customer
merge into dwh.d_customer as dcs
using
    (
        select distinct
            customer_name
            , customer_address
            , customer_birthday
            , customer_email
        from tmp_sources
    ) as tmp
    on
        dcs.customer_name = tmp.customer_name
        and dcs.customer_email = tmp.customer_email
when matched then
    update set
        customer_address = tmp.customer_address
        , customer_birthday = tmp.customer_birthday
        , load_dttm = current_timestamp
when not matched then
    insert
        (
            customer_name
            , customer_address
            , customer_birthday
            , customer_email
            , load_dttm
        )
    values (tmp.customer_name, tmp.customer_address, tmp.customer_birthday, tmp.customer_email, current_timestamp);

-- создание временной таблицы tmp_sources_fact с соединенными данными
drop table if exists tmp_sources_fact;
create temp table tmp_sources_fact as
select
    dp.product_id
    , dcf.craftsman_id
    , dcs.customer_id
    , src.order_created_date
    , src.order_completion_date
    , src.order_status
    , current_timestamp
from tmp_sources as src
inner join dwh.d_craftsman as dcf
    on
        src.craftsman_name = dcf.craftsman_name
        and src.craftsman_email = dcf.craftsman_email
inner join dwh.d_customer as dcs
    on
        src.customer_name = dcs.customer_name
        and src.customer_email = dcs.customer_email
inner join dwh.d_product as dp
    on
        src.product_name = dp.product_name
        and src.product_description = dp.product_description
        and src.product_price = dp.product_price;

-- обновление существующих записей и добавление новых в витрину dwh.f_order
merge into dwh.f_order as fo
using tmp_sources_fact as tmp
    on
        fo.product_id = tmp.product_id
        and fo.craftsman_id = tmp.craftsman_id
        and fo.customer_id = tmp.customer_id
        and fo.order_created_date = tmp.order_created_date
when matched then
    update set
        order_completion_date = tmp.order_completion_date
        , order_status = tmp.order_status
        , load_dttm = current_timestamp
when not matched then
    insert
        (
            product_id
            , craftsman_id
            , customer_id
            , order_created_date
            , order_completion_date
            , order_status
            , load_dttm
        )
    values (tmp.product_id, tmp.craftsman_id, tmp.customer_id, tmp.order_created_date, tmp.order_completion_date, tmp.order_status, current_timestamp);
