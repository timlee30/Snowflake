
  
    

        create or replace transient table dbt_hol_2025_prod.public_01_staging.stg_trading_books
         as
        (with source as (
    select * from dbt_hol_2025_prod.public.trading_books
),

renamed as (
    select
        trade_id,
        trade_date,
        trader_name,
        desk,
        ticker,
        quantity,
        price,
        trade_type,
        notes
    from source
)

select * from renamed
        );
      
  