merge into STV202310160__DWH.global_metrics as gm
using (
	select 
		t.transaction_dt as date_update,
		t.currency_code as currency_from ,
		sum(t.amount_with_curr_div) as amount_total,
		count(distinct operation_id) as cnt_transactions,
		count(distinct operation_id)/count(distinct account_number) as avg_transactions_per_account,
		count(distinct account_number) as cnt_accounts_make_transactions
	from (
		select
			t.transaction_dt::date,
			t.operation_id,
			t.account_number_from,
			account_number_to,
			t.currency_code,
			t.amount,
			t.transaction_type,
			t.status,
			case 
				when right(transaction_type, 8) = 'outgoing' 
				then account_number_from
				else account_number_to
			end as account_number,
			c.currency_with_div,
			case 
				when t.currency_code = 420 then t.amount
				else t.amount * c.currency_with_div
			end as amount_with_curr_div
		from STV202310160__STAGING.transactions t 
		left join STV202310160__STAGING.currencies c
			on t.transaction_dt::date = c.date_update::date
			and t.currency_code = c.currency_code 
			and c.currency_code_with = 420
		where 
			t.transaction_dt::date = '{date}'::date-1
			and t.account_number_from >= 0
			and t.status = 'done'
		) as t
	group by 
		t.transaction_dt,
		t.currency_code
    ) as src
on 
	gm.date_update = src.date_update
	and gm.currency_from = src.currency_from
when matched 
	then update set 
	currency_from = src.currency_from,
    amount_total = src.amount_total, 
    cnt_transactions = src.cnt_transactions,  
    avg_transactions_per_account = src.avg_transactions_per_account,
    cnt_accounts_make_transactions = src.cnt_accounts_make_transactions
when not matched 
	then insert (
		date_update, 
		currency_from, 
		amount_total, 
		cnt_transactions, 
		avg_transactions_per_account, 
		cnt_accounts_make_transactions
	)
	values (
		src.date_update, 
		src.currency_from, 
		src.amount_total, 
		src.cnt_transactions, 
		src.avg_transactions_per_account, 
		src.cnt_accounts_make_transactions
	)
;