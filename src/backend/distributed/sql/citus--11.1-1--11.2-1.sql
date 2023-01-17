-- citus--11.1-1--11.2-1

DROP FUNCTION pg_catalog.worker_append_table_to_shard(text, text, text, integer);

#include "udfs/get_rebalance_progress/11.2-1.sql"
#include "udfs/citus_isolation_test_session_is_blocked/11.2-1.sql"
#include "datatypes/citus_cluster_clock/11.2-1.sql"
#include "udfs/citus_get_node_clock/11.2-1.sql"
#include "udfs/citus_get_transaction_clock/11.2-1.sql"
#include "udfs/citus_is_clock_after/11.2-1.sql"
#include "udfs/citus_internal_adjust_local_clock_to_remote/11.2-1.sql"
#include "udfs/citus_job_list/11.2-1.sql"
#include "udfs/citus_job_status/11.2-1.sql"
#include "udfs/citus_rebalance_status/11.2-1.sql"
#include "udfs/worker_split_shard_replication_setup/11.2-1.sql"
#include "udfs/citus_task_wait/11.2-1.sql"
#include "udfs/citus_prepare_pg_upgrade/11.2-1.sql"
#include "udfs/citus_finish_pg_upgrade/11.2-1.sql"
#include "udfs/citus_copy_shard_placement/11.2-1.sql"
#include "udfs/citus_move_shard_placement/11.2-1.sql"

-- drop orphaned shards after inserting records for them into pg_dist_cleanup
INSERT INTO pg_dist_cleanup
    SELECT nextval('pg_dist_cleanup_recordid_seq'), 0, 1, shard_name(sh.logicalrelid, sh.shardid) AS object_name, plc.groupid AS node_group_id, 0
        FROM pg_dist_placement plc
        JOIN pg_dist_shard sh ON sh.shardid = plc.shardid
        WHERE plc.shardstate = 4;

DELETE FROM pg_dist_placement WHERE shardstate = 4;

/*
 * Use the following DDLs for testing:

CREATE TABLE companies (
  id bigserial PRIMARY KEY,
  name text NOT NULL,
  image_url text,
  created_at timestamp without time zone NOT NULL,
  updated_at timestamp without time zone NOT NULL
);

CREATE TABLE campaigns (
  id bigserial,       -- was: PRIMARY KEY
  company_id bigint REFERENCES companies (id),
  name text NOT NULL,
  cost_model text NOT NULL,
  state text NOT NULL,
  monthly_budget bigint,
  blacklisted_site_urls text[],
  created_at timestamp without time zone NOT NULL,
  updated_at timestamp without time zone NOT NULL,
  PRIMARY KEY (company_id, id) -- added
);

CREATE TABLE ads (
  id bigserial,       -- was: PRIMARY KEY
  company_id bigint,  -- added
  campaign_id bigint, -- was: REFERENCES campaigns (id)
  name text NOT NULL,
  image_url text,
  target_url text,
  impressions_count bigint DEFAULT 0,
  clicks_count bigint DEFAULT 0,
  created_at timestamp without time zone NOT NULL,
  updated_at timestamp without time zone NOT NULL,
  PRIMARY KEY (company_id, id),         -- added
  FOREIGN KEY (company_id, campaign_id) -- added
    REFERENCES campaigns (company_id, id)
);

CREATE TABLE clicks (
  id bigserial,        -- was: PRIMARY KEY
  company_id bigint,   -- added
  ad_id bigint,        -- was: REFERENCES ads (id),
  clicked_at timestamp without time zone NOT NULL,
  site_url text NOT NULL,
  cost_per_click_usd numeric(20,10),
  user_ip inet NOT NULL,
  user_data jsonb NOT NULL,
  PRIMARY KEY (company_id, id),      -- added
  FOREIGN KEY (company_id, ad_id)    -- added
    REFERENCES ads (company_id, id)
);

CREATE TABLE impressions (
  id bigserial,         -- was: PRIMARY KEY
  company_id bigint,    -- added
  ad_id bigint,         -- was: REFERENCES ads (id),
  seen_at timestamp without time zone NOT NULL,
  site_url text NOT NULL,
  cost_per_impression_usd numeric(20,10),
  user_ip inet NOT NULL,
  user_data jsonb NOT NULL,
  PRIMARY KEY (company_id, id),       -- added
  FOREIGN KEY (company_id, ad_id)     -- added
    REFERENCES ads (company_id, id)
);

 */

CREATE OR REPLACE VIEW citus_shard_key_candidates AS
WITH fkeys AS (
        select
            c.oid as reloid, cf.oid as freloid,
            (select string_agg(attname, ',')
            from pg_attribute a
            where a.attrelid = r.conrelid
                and array[a.attnum::integer] <@ conkey::integer[]
            ) as conkey,
            (select string_agg(attname, ',')
            from pg_attribute a
            where a.attrelid = r.confrelid
                and array[a.attnum::integer] <@ confkey::integer[]
            ) as confkey
    from pg_catalog.pg_constraint r
            JOIN pg_class c on r.conrelid = c.oid
            JOIN pg_namespace n on c.relnamespace = n.oid
            JOIN pg_class cf on r.confrelid = cf.oid
            JOIN pg_namespace nf on cf.relnamespace = nf.oid
            JOIN pg_depend d on d.classid = 'pg_constraint'::regclass
                            and d.objid = r.oid
                            and d.refobjsubid = 0
    where r.contype = 'f'
            AND n.nspname !~ '^pg_' and n.nspname <> 'information_schema'
            AND nf.nspname !~ '^pg_' and nf.nspname <> 'information_schema'
)
select n.nspname as schema_name, c.relname as table_name,
       CASE WHEN pkeys.attname IS NULL THEN 'reference' ELSE 'distributed' END AS citus_table_type,
       pkeys.attname as distribution_key,
       CASE
         WHEN COUNT(fkeys_d.reloid) > 0
         THEN json_agg(json_object(array['table', 'column'], array[fkeys_d.reloid::regclass::text, fkeys_d.conkey]) order by fkeys_d.reloid::regclass::text)
         WHEN COUNT(fkeys_r.freloid) > 0
         THEN json_agg(json_object(array['table', 'column'], array[fkeys_r.freloid::regclass::text, fkeys_r.confkey]) order by fkeys_r.freloid::regclass::text)
       END as related_columns
  from pg_catalog.pg_class c
       join pg_catalog.pg_namespace n on c.relnamespace = n.oid
       join pg_roles auth ON auth.oid = c.relowner
       left join lateral (
           select indrelid, indexrelid, a.attname
             from pg_index x
             join pg_class i on i.oid = x.indexrelid
             join pg_attribute a on a.attrelid = c.oid and attnum = indkey[0]
            where x.indrelid = c.oid
              and (indisprimary or indisunique)
              and array_length(indkey::integer[], 1) = 1
              and atttypid in ('smallint'::regtype,
                               'int'::regtype,
                               'bigint'::regtype)
         order by not indisprimary, not indisunique
            limit 1
       ) as pkeys on true
  left join fkeys AS fkeys_d ON (fkeys_d.freloid = c.oid)
  left join fkeys AS fkeys_r ON (fkeys_r.reloid = c.oid)
 where relkind = 'r'
   and n.nspname !~ '^pg_' and n.nspname <> 'information_schema'
   and not exists
     (
       select 1
         from pg_depend d
        where d.classid = 'pg_class'::regclass
          and d.objid = c.oid
          and d.deptype = 'e'
     )
   and not exists (select 1 from pg_dist_partition where logicalrelid = c.oid)
group by schema_name,table_name,citus_table_type,distribution_key
order by citus_table_type, n.nspname, c.relname;

