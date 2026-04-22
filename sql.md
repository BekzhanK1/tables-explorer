SELECT
  c.table_schema,
  c.table_name,
  c.column_name,
  c.data_type,
  c.character_maximum_length,
  c.is_nullable,
  c.column_default,
  pg_catalog.col_description(
    format('%I.%I', c.table_schema, c.table_name)::regclass,
    c.ordinal_position
  ) AS column_description
FROM information_schema.columns c
WHERE c.table_schema NOT IN ('pg_catalog', 'information_schema')
ORDER BY c.table_schema, c.table_name, c.ordinal_position;