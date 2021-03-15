INSERT INTO dim_date(date_key, date_alternate_key, year, quarter, month_name, month, day) (
  SELECT
    cast(to_char("date", 'yyyymmdd') as INTEGER),
    "date",
    extract(year from "date"),
    extract(quarter from "date"),
    to_char("date", 'Month'),
    extract(month from  "date"),
    extract(day from "date") 
  FROM staging_sales_all
  WHERE date >= '{{params.date}}' AND date <= '{{params.date}}'
)
ON CONFLICT (date_key) 
DO NOTHING;