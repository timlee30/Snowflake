with source as (
    select * from STOCK_TRACKING_US_STOCK_PRICES_BY_DAY.STOCK.US_STOCK_METRICS
),

renamed as (
    select
        run_date,
        ticker,
        open as open_price,
        high as high_price,
        low as low_price,
        close as close_price,
        volume
    from source
)

select * from renamed 
where 1=1 
  AND run_date >= '2024-01-01'