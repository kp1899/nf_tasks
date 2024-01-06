insert into ds.md_exchange_rate_d
    select
        id::int as id,
        data_actual_date::date as data_actual_date,
        data_actual_end_date::date as data_actual_end_date,
        currency_rk::int as currency_rk,
        reduced_cource::double precision as currency_code,
        code_iso_num::text as code_iso_char

    from ds.md_exchange_rate_d_tmp
on conflict (id, data_actual_date, currency_rk) do update
    set
        data_actual_end_date = excluded.data_actual_end_date,
        reduced_cource = excluded.reduced_cource,
        code_iso_num = excluded.code_iso_num
;
