create table if not exists ds.ft_posting_f (
    id int not null,
    oper_date date not null,
    credit_account_rk int not null,
    debet_account_rk int not null,
    credit_amount double precision,
    debet_amount double precision,
    primary key (id, oper_date, credit_account_rk, debet_account_rk)
);

