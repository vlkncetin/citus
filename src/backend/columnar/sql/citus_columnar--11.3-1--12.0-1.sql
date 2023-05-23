CREATE OR REPLACE FUNCTION pg_catalog.duckdb_query(query text)
 RETURNS SETOF record
 LANGUAGE c
 STRICT
AS 'MODULE_PATHNAME', $function$run_duckdb_query$function$
