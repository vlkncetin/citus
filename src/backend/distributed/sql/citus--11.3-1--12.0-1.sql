-- citus--11.3-1--12.0-1

-- bump version to 12.0-1

CREATE OR REPLACE FUNCTION pg_catalog.get_cluster_stats()
RETURNS text
AS $$
DECLARE
    cluster_stats text;
BEGIN
    WITH
    citus_tables_arr AS (
        SELECT array_agg(row_to_json(t.*)) d FROM (
            SELECT table_name, distribution_column FROM public.citus_tables
        ) t
    ),
    all_tables_arr AS (
        SELECT array_agg(tablename) d from pg_tables where schemaname='public'
    ),
    citus_nodes_arr AS (
        SELECT array_agg(row_to_json(t.*)) d FROM (
            SELECT nodename, nodeport FROM pg_catalog.pg_dist_node
        ) t
    ),
    key_values AS (
        SELECT
          unnest(ARRAY[
              'citus_tables',
              'all_tables',
              'citus_nodes'
          ]) AS key,
          unnest(ARRAY[
              to_json(citus_tables_arr.d),
              to_json(all_tables_arr.d),
              to_json(citus_nodes_arr.d)
          ]::json[]) AS value
        FROM
          citus_tables_arr,
          all_tables_arr,
          citus_nodes_arr
    )
    SELECT json_object_agg(key, value)::text INTO cluster_stats
    FROM key_values;

    RETURN cluster_stats;
END;
$$ LANGUAGE plpgsql;
