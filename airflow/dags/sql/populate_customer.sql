INSERT INTO dim_customer (customer_alternate_key, customer_name, customer_birthdate) (
  SELECT customer_id, customer_name, customer_birthdate
  FROM staging_customer
)
ON CONFLICT (customer_alternate_key) 
DO 
  UPDATE SET customer_name = excluded.customer_name, customer_birthdate = excluded.customer_birthdate WHERE (dim_customer.customer_name <>  excluded.customer_name OR dim_customer.customer_birthdate <>  excluded.customer_birthdate );