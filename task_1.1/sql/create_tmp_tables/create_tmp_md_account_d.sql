drop table if exists ds.md_account_d_tmp;
create table if not exists ds.md_account_d_tmp (
    id text,
    data_actual_date text,
    data_actual_end_date text,
    account_rk text,
    account_number text,
    char_type text,
    currency_rk text,
    currency_code text
);
