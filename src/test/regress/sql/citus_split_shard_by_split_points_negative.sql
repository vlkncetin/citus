-- Negative test cases for citus_split_shard_by_split_points UDF.

CREATE SCHEMA citus_split_shard_by_split_points_negative;
SET search_path TO citus_split_shard_by_split_points_negative;
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 60761300;

CREATE TABLE range_paritioned_table_to_split(rid bigserial PRIMARY KEY, value char);
SELECT create_distributed_table('range_paritioned_table_to_split', 'rid', 'range');
-- Shards are not created automatically for range distributed table.
SELECT master_create_empty_shard('range_paritioned_table_to_split');

SET citus.next_shard_id TO 49761300;
CREATE TABLE table_to_split (id bigserial PRIMARY KEY, value char);

-- Shard1     | -2147483648   | -1073741825
-- Shard2     | -1073741824   | -1
-- Shard3     | 0             | 1073741823
-- Shard4     | 1073741824    | 2147483647
SELECT create_distributed_table('table_to_split','id');

SELECT nodeid AS worker_1_node FROM pg_dist_node WHERE nodeport=:worker_1_port \gset
SELECT nodeid AS worker_2_node FROM pg_dist_node WHERE nodeport=:worker_2_port \gset

-- UDF fails for range partitioned tables.
SELECT citus_split_shard_by_split_points(
	60761300,
	ARRAY['-1073741826'],
	ARRAY[:worker_1_node, :worker_2_node]);

-- UDF fails if number of placement node list does not exceed split points by one.
-- Example: One split point defines two way split (2 worker nodes needed).
SELECT citus_split_shard_by_split_points(
	49761300,
    -- 2 split points defined making it a 3 way split but we only specify 2 placement lists.
	ARRAY['-1073741826', '-107374182'],
	ARRAY[:worker_1_node, :worker_2_node]); -- 2 worker nodes.

-- UDF fails if split ranges specified are not within the shard id to split.
SELECT citus_split_shard_by_split_points(
	49761300, -- Shard range is from (-2147483648, -1073741825)
	ARRAY['0'], -- The range we specified is 0 which is not in the range.
	ARRAY[:worker_1_node, :worker_2_node]);

-- UDF fails if split points are not strictly increasing.
SELECT citus_split_shard_by_split_points(
	49761302,
	ARRAY['50', '35'],
    ARRAY[:worker_1_node, :worker_2_node, :worker_1_node]);

SELECT citus_split_shard_by_split_points(
	49761302,
	ARRAY['50', '50'],
	ARRAY[:worker_1_node, :worker_2_node, :worker_1_node]);

-- UDF fails if nodeIds are < 1 or Invalid.
SELECT citus_split_shard_by_split_points(
	49761302,
	ARRAY['50'],
	ARRAY[0, :worker_2_node]);

SELECT citus_split_shard_by_split_points(
	49761302,
	ARRAY['50'],
	ARRAY[101, 201]);

-- UDF fails if split point specified is equal to the max value in the range.
-- Example: ShardId 81060002 range is from (-2147483648, -1073741825)
-- '-1073741825' as split point is invalid.
-- '-1073741826' is valid and will split to: (-2147483648, -1073741826) and (-1073741825, -1073741825)
SELECT citus_split_shard_by_split_points(
	49761300, -- Shard range is from (-2147483648, -1073741825)
	ARRAY['-1073741825'], -- Split point equals shard's max value.
	ARRAY[:worker_1_node, :worker_2_node]);

-- UDF fails where source shard cannot be split further i.e min and max range is equal.
-- Create a Shard where range cannot be split further
SELECT isolate_tenant_to_new_shard('table_to_split', 1);
SELECT citus_split_shard_by_split_points(
	49761305,
	ARRAY['-1073741826'],
	ARRAY[:worker_1_node, :worker_2_node]);

-- Create distributed table with replication factor > 1
SET citus.shard_replication_factor TO 2;
SET citus.next_shard_id TO 51261400;
CREATE TABLE table_to_split_replication_factor_2 (id bigserial PRIMARY KEY, value char);
SELECT create_distributed_table('table_to_split_replication_factor_2','id');

-- UDF fails for replication factor > 1
SELECT citus_split_shard_by_split_points(
	51261400,
	ARRAY['-1073741826'],
	ARRAY[:worker_1_node, :worker_2_node]);

-- Create distributed table with columnar type.
SET citus.next_shard_id TO 51271400;
CREATE TABLE table_to_split_columnar (id bigserial PRIMARY KEY, value char) USING columnar;
SELECT create_distributed_table('table_to_split_columnar','id');

-- UDF fails for columnar table.
SELECT citus_split_shard_by_split_points(
	51271400,
	ARRAY['-1073741826'],
	ARRAY[:worker_1_node, :worker_2_node]);

-- Create distributed table which is partitioned.
SET citus.next_shard_id TO 51271900;
CREATE TABLE table_to_split_partitioned(id integer, dt date) PARTITION BY RANGE(dt);
SELECT create_distributed_table('table_to_split_partitioned','id');

-- UDF fails for partitioned table.
SELECT citus_split_shard_by_split_points(
	51271900,
	ARRAY['-1073741826'],
	ARRAY[:worker_1_node, :worker_2_node]);