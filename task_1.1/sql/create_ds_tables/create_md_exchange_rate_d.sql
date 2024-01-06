create table if not exists ds.md_exchange_rate_d (
    id int,
    data_actual_date date not null,
    data_actual_end_date date,
    currency_rk int not null,
    reduced_cource double precision,
    code_iso_num varchar(3),
    primary key (id, data_actual_date, currency_rk)
);
