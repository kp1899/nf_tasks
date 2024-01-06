insert into ds.ft_posting_f
    select
        id::int as id,
        oper_date::date as oper_date,
        credit_account_rk::int as credit_account_rk,
        debet_account_rk::int as debet_account_rk,
        credit_amount::double precision as credit_amount,
        debet_amount::double precision as debet_amount

    from ds.ft_posting_f_tmp
on conflict (id, oper_date, credit_account_rk, debet_account_rk) do update
    set
        credit_amount = excluded.credit_amount,
        debet_amount = excluded.debet_amount;
