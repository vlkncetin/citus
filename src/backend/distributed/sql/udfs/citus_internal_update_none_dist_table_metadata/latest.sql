CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_update_none_dist_table_metadata(relation_id oid, replication_model "char", colocation_id bigint)
    RETURNS void
    LANGUAGE C
    VOLATILE
    AS 'MODULE_PATHNAME';
