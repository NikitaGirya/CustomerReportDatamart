-- DDL витрины dwh.customer_report_datamart

drop table if exists dwh.customer_report_datamart;
create table if not exists dwh.customer_report_datamart 
	(
	id bigint generated always as identity not null
	, customer_id int8 not null
	, customer_name varchar not null
	, customer_address varchar not null
	, customer_birthday date not null
	, customer_email varchar not null
	, customer_paid_money bigint not null
	, platform_received_money numeric(15, 2) not null 
	, count_order int8 not null
	, avg_price_order numeric(10, 2) not null
	, median_time_order_completed numeric(10, 1) null
	, top_product_category varchar not null
	, top_craftsman_id int8 not null
	, count_order_created int8 not null
	, count_order_in_progress int8 not null
	, count_order_delivery int8 not null
	, count_order_done int8 not null
	, count_order_not_done int8 not null
	, report_period varchar not null
	, constraint customer_report_datamart_pk primary key (id)
	);