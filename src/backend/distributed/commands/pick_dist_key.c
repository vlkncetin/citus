#include "postgres.h"
#include "fmgr.h"
#include "storage/lockdefs.h"
#include "utils/relcache.h"

PG_FUNCTION_INFO_V1(pick_dist_key);

Datum
pick_dist_key(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0))
	{
		ereport(ERROR, (errmsg("table name cannot be null")));
	}

	Oid relationId = PG_GETARG_OID(0);

	Relation relation = try_relation_open(relationId, AccessShareLock);
	if (relation == NULL)
	{
		ereport(ERROR, (errmsg("no such table exists")));
	}

	relation_close(relation, NoLock);

	char *distributionColumnName = text_to_cstring(distributionColumnText);
	Assert(distributionColumnName != NULL);

	char distributionMethod = LookupDistributionMethod(distributionMethodOid);

	if (shardCount < 1 || shardCount > MAX_SHARD_COUNT)
	{
		ereport(ERROR, (errmsg("%d is outside the valid range for "
							   "parameter \"shard_count\" (1 .. %d)",
							   shardCount, MAX_SHARD_COUNT)));
	}

	CreateDistributedTable(relationId, distributionColumnName, distributionMethod,
						   shardCount, shardCountIsStrict, colocateWithTableName);

	PG_RETURN_VOID();
}
