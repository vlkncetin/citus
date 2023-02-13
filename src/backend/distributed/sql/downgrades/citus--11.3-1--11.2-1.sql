-- citus--11.3-1--11.2-1
-- this is an empty downgrade path since citus--11.2-1--11.3-1.sql is empty for now

DROP FUNCTION citus_internal_add_colocation_metadata (integer, integer, integer, regtype, oid, regnamespace, integer);
#include "../udfs/citus_internal_add_colocation_metadata/11.0-1.sql"

DROP INDEX pg_dist_colocation_schema_index;

ALTER TABLE pg_catalog.pg_dist_colocation DROP COLUMN associatedschema;
ALTER TABLE pg_catalog.pg_dist_colocation DROP COLUMN associatedgroupid;
