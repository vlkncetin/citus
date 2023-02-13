-- citus--11.2-1--11.3-1

-- bump version to 11.3-1

ALTER TABLE pg_catalog.pg_dist_colocation ADD COLUMN associatedschema OID;
ALTER TABLE pg_catalog.pg_dist_colocation ADD COLUMN associatedgroupid INTEGER;

-- XXX: Can replace this with a UNIQUE NULLS NOT DISTINCT constraint after we drop support for PG < 15
CREATE INDEX pg_dist_colocation_schema_index ON pg_catalog.pg_dist_colocation(associatedschema);

DROP FUNCTION citus_internal_add_colocation_metadata (integer, integer, integer, regtype, oid);
#include "udfs/citus_internal_add_colocation_metadata/11.3-1.sql"
