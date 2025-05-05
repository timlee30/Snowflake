with source as (
    select *
    from dbt_hol_2025_prod.public.weights_table
),
renamed as (
    select region,
        desk,
        target_allocation
    from source
)
select *
from renamed