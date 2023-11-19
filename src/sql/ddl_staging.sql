--transactions
drop table if exists STV202310160__STAGING.transactions;

create table STV202310160__STAGING.transactions (
	operation_id varchar(60) primary key,
	account_number_from int,
	account_number_to int,
	currency_code int,
	country varchar(30),
	status varchar(30),
	transaction_type varchar(30),
	amount int,
	transaction_dt timestamp(3) 
)
order by transaction_dt, operation_id
segmented by hash(transaction_dt, operation_id) all nodes
partition by transaction_dt::date
group by calendar_hierarchy_day(transaction_dt::date, 3, 2)
;

--currencies
drop table if exists STV202310160__STAGING.currencies;

create table STV202310160__STAGING.currencies (
	date_update timestamp(3),
	currency_code int,
	currency_code_with int,
	currency_with_div numeric(5, 3)
)
order by date_update, currency_code, currency_code_with
segmented by hash(date_update, currency_code, currency_code_with) all nodes
partition by date_update::date
group by calendar_hierarchy_day(date_update::date, 3, 2)
;