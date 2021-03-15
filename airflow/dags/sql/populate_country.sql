INSERT INTO dim_country (country_alternate_key, country_name) (
  SELECT country_id, country_name
  FROM staging_country
)
ON CONFLICT (country_alternate_key) 
DO 
  UPDATE SET country_name = excluded.country_name WHERE dim_country.country_name <> excluded.country_name;