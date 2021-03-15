WITH inserts AS (
      SELECT *
      FROM staging_product sp
      WHERE NOT EXISTS (SELECT 1
                        FROM dim_product p
                        WHERE p.product_alternate_key = sp.product_code AND
                              p.product_description = sp.product_description AND 
                              p.unit_price = sp.unit_price
                      )
     ),
     updates AS (
      UPDATE dim_product p
      SET is_current = 0,
          valid_to = cast(to_char(NOW(), 'yyyymmdd') as INTEGER)
      FROM inserts
      where inserts.product_code = p.product_alternate_key AND
            p.is_current = 1
     )
INSERT into dim_product (product_alternate_key, product_description, unit_price,  valid_from, valid_to, is_current) (
  SELECT product_code, product_description, unit_price, cast(to_char(NOW(), 'yyyymmdd') as INTEGER), 99991231, 1
  FROM inserts
);
