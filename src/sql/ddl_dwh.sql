create table STV202310160__DWH.global_metrics (
	date_update timestamp(3),
	currency_from int,
	amount_total int,
	cnt_transactions int,
	avg_transactions_per_account numeric(5, 3),
	cnt_accounts_make_transactions int
)
order by date_update
segmented by hash(date_update) all nodes
partition by date_update::date
group by calendar_hierarchy_day(date_update::date, 3, 2)
;