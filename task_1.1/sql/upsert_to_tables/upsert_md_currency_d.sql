insert into ds.md_currency_d
    select
        currency_rk::int as currency_rk,
        data_actual_date::date as data_actual_date,
        data_actual_end_date::date as data_actual_end_date,
        currency_code::text as currency_code,
        code_iso_char::text as code_iso_char

    from ds.md_currency_d_tmp
on conflict (currency_rk, data_actual_date) do update
    set
        data_actual_end_date = excluded.data_actual_end_date,
        currency_code = excluded.currency_code,
        code_iso_char = excluded.code_iso_char
;
