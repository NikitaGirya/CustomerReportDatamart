-- DDL таблицы инкрементальных загрузок dwh.load_dates_customer_report_datamart

drop table if exists dwh.load_dates_customer_report_datamart;
create table if not exists dwh.load_dates_customer_report_datamart 
	(
    id bigint generated always as identity not null
    , load_dttm timestamp not null
    , constraint load_dates_customer_report_datamart_pk primary key (id)
	);