insert into ds.md_account_d
    select
        data_actual_date::date as data_actual_date,
        data_actual_end_date::date as data_actual_end_date,
        account_rk::int as account_rk,
        account_number::text as account_number,
        char_type::text as char_type,
        currency_rk::int as currency_rk,
        currency_code::text as currency_code

    from ds.md_account_d_tmp
on conflict (data_actual_date, account_rk) do update
    set
        data_actual_end_date = excluded.data_actual_end_date,
        account_number = excluded.account_number,
        char_type = excluded.char_type,
        currency_rk = excluded.currency_rk,
        currency_code = excluded.currency_code
;
