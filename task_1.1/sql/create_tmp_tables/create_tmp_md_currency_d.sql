drop table if exists ds.md_currency_d_tmp;
create table if not exists ds.md_currency_d_tmp (
    id text,
    currency_rk text,
    data_actual_date text,
    data_actual_end_date text,
    currency_code text,
    code_iso_char text
);
