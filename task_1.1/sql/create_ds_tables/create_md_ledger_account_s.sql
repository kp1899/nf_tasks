create table if not exists ds.md_ledger_account_s (
    chapter char(1),
    chapter_name varchar(16),
    section_number integer,
    section_name varchar(22),
    subsection_name varchar(21),
    ledger1_account integer,
    ledger1_account_name varchar(47),
    ledger_account integer not null,
    ledger_account_name varchar(153),
    characteristic char(1),
    is_resident integer,
    is_reserve integer,
    is_reserved integer,
    is_loan integer,
    is_reserved_assets integer,
    is_overdue integer,
    is_interest integer,
    pair_account varchar(5),
    start_date date not null,
    end_date date,
    is_rub_only integer,
    min_term varchar(1),
    min_term_measure varchar(1),
    max_term varchar(1),
    max_term_measure varchar(1),
    ledger_acc_full_name_translit varchar(1),
    is_revaluation varchar(1),
    is_correct varchar(1),
    primary key (ledger_account, start_date)
);