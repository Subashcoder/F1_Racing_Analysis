select 
order_key,
sum(extended_price) as gross_price,
sum(discount_amount) as gross_discount
FROM
    {{ref('int_order_item')}}

group by order_key