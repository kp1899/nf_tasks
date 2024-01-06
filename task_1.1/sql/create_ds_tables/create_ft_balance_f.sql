create table if not exists ds.ft_balance_f (
    on_date date not null,
    account_rk int not null,
    currency_rk int,
    balance_out double precision,
    primary key (on_date, account_rk)
);
