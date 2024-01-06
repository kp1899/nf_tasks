drop table if exists ds.ft_posting_f_tmp;
create table if not exists ds.ft_posting_f_tmp (
    id text,
    oper_date text,
    credit_account_rk text,
    debet_account_rk text,
    credit_amount text,
    debet_amount text
);
