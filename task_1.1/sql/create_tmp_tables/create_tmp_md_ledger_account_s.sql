drop table if exists ds.md_ledger_account_s_tmp;
create table if not exists ds.md_ledger_account_s_tmp (
    id text,
    chapter text,
    chapter_name text,
    section_number text,
    section_name text,
    subsection_name text,
    ledger1_account text,
    ledger1_account_name text,
    ledger_account text,
    ledger_account_name text,
    characteristic text,
    is_resident text,
    is_reserve text,
    is_reserved text,
    is_loan text,
    is_reserved_assets text,
    is_overdue text,
    is_interest text,
    pair_account text,
    start_date text,
    end_date text,
    is_rub_only text,
    min_term text,
    min_term_measure text,
    max_term text,
    max_term_measure text,
    ledger_acc_full_name_translit text,
    is_revaluation text,
    is_correct text
);
