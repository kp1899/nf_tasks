insert into ds.ft_balance_f
    select
        to_date(on_date, 'DD.MM.YYYY') as on_date,
        account_rk::int as account_rk,
        currency_rk::int as currency_rk,
        balance_out::double precision as balance_out

    from ds.ft_balance_f_tmp
on conflict (on_date, account_rk) do update
    set
        currency_rk = excluded.currency_rk,
        balance_out = excluded.balance_out;
