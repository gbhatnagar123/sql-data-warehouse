--Quality checks
select * from gold.dim_customers

select * from gold.fact_sales f
left join gold.dim_customers c
on c.customer_key = f.customer_key
where c.customer_key is null

select * from gold.fact_sales f
left join gold.dim_products p
on p.product_key = f.product_key
where p.product_key is null
