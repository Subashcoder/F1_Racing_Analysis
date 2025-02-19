select 
orders.*,
summary.gross_price,
summary.gross_discount

from 
    {{ref('stg_tpch_orders')}} as orders
join {{ref('int_order_summary')}} as summary
on orders.order_key = summary.order_key