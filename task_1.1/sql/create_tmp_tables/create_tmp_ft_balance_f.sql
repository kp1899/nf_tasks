drop table if exists ds.ft_balance_f_tmp;
create table ds.ft_balance_f_tmp (
    "id" text,
    "on_date" text,
    "account_rk" text,
    "currency_rk" text,
    "balance_out" text
);
