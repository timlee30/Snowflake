
    
    

select
    TRADE_ID as unique_field,
    count(*) as n_records

from dbt_hol_2025_dev.public.trading_books
where TRADE_ID is not null
group by TRADE_ID
having count(*) > 1


