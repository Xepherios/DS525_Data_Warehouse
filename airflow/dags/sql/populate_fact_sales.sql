INSERT INTO fact_sales (date_key, customer_key, product_key, country_key, unit_price, quantity, sales_amount, invoice_no) (
  SELECT 
    dim_date.date_key, 
    dim_customer.customer_key,
    dim_product.product_key,
    dim_country.country_key,
    staging_sales_all.unit_price,
    staging_sales_all.quantity,
    staging_sales_all.unit_price * staging_sales_all.quantity,
    staging_sales_all.invoice_no
  FROM staging_sales_all
  LEFT OUTER JOIN dim_date ON staging_sales_all.date = dim_date.date_alternate_key
  LEFT OUTER JOIN dim_customer ON staging_sales_all.customer_id = dim_customer.customer_alternate_key
  LEFT OUTER JOIN dim_product ON staging_sales_all.product_code = dim_product.product_alternate_key
  LEFT OUTER JOIN dim_country ON staging_sales_all.country_id = dim_country.country_alternate_key
  WHERE dim_product.is_current = 1
)
