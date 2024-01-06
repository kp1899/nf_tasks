drop table if exists ds.md_exchange_rate_d_tmp;
create table if not exists ds.md_exchange_rate_d_tmp (
    id text,
    data_actual_date text,
    data_actual_end_date text,
    currency_rk text,
    reduced_cource text,
    code_iso_num text
);
