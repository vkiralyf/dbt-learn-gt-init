with orders as (

    select * from {{ ref('stg_jaffle_shop_orders') }}

),

payments as (

    select * from {{ ref('stg_stripe__payments') }}

),
order_payments as (
    select 
    order_ID, SUM(CASE WHEN status = 'success' THEN amount ELSE 0 END) as amount
    from payments
    group by 1
),
final as(
    select
    orders.order_id,
    orders.customer_id,
    orders.order_date,
    coalesce(order_payments.amount, 0) as amount
    from orders
    left join order_payments using (order_id)
)
select * from final