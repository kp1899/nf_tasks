create table if not exists ds.md_currency_d (
    currency_rk int not null,
    data_actual_date date not null,
    data_actual_end_date date,
    currency_code varchar(3),
    code_iso_char varchar(3),
    primary key (currency_rk, data_actual_date)
);
