select 
    o_orderkey as order_key,
    o_custkey as customer_key,
    o_orderstatus as oderstatus,
    o_totalprice as totalprice,
    o_orderdate as oderdate,
    o_comment as comment
 FROM
    {{ source( 'tpch', 'orders') }}