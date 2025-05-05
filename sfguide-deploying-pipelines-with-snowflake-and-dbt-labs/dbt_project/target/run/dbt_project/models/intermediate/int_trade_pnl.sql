
        

    
        create dynamic table dbt_hol_2025_prod.public_02_intermediate.int_trade_pnl
        target_lag = 'downstream'
        warehouse = VWH_DBT_HOL
        refresh_mode = INCREMENTAL

        initialize = ON_CREATE

        as (
            with equity_trades as (
    select * from dbt_hol_2025_prod.public_02_intermediate.int_equity_trade_pnl
),

fx_trades as (
    select * from dbt_hol_2025_prod.public_02_intermediate.int_fx_trade_pnl
)

select * from equity_trades
union all
select * from fx_trades
        )

    


    