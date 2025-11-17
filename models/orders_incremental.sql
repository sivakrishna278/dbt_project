{{
    config 
    (
        materialized ='incremental',
        unique_key ='order_id',
        incremental_strategy='merge'
    )
}}
with source as
(
    select order_id,
    customer_id,
    order_amount,
    status,
    updated_at from {{source('sales','orders')}}
)
select *from source

{% if is_incremental() %}
where updated_at>(select max(updated_at) from {{this}})
{% endif %} 