create table if not exists ds.md_account_d (
    data_actual_date date not null,
    data_actual_end_date date not null,
    account_rk int not null,
    account_number varchar(20) not null,
    char_type varchar(1) not null,
    currency_rk int not null,
    currency_code varchar(3) not null,
    primary key (data_actual_date, account_rk)
);
