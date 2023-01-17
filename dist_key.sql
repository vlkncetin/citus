-- CREATE VIEW citus_shard_key_candidates AS
WITH initial_suggestions AS (
    WITH fkeys AS (
        select distinct on (freloid, confkey)
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
            AND c.relkind in ('r', 'f', 'p')
            AND cf.relkind in ('r', 'f', 'p')
            AND n.nspname !~ '^pg_' and n.nspname <> 'information_schema'
            AND nf.nspname !~ '^pg_' and nf.nspname <> 'information_schema'
            AND c.relkind = 'r' and c.relpersistence = 'p'
            AND cf.relkind = 'r' and cf.relpersistence = 'p'
    )
    select c.oid as table_oid,
           CASE WHEN pkeys.attname IS NULL THEN 'reference' ELSE 'distributed' END AS citus_table_type,
           pkeys.attname as distribution_key,
           CASE
             WHEN pkeys.attname IS NOT NULL
             THEN json_agg(json_object(array['table', 'column'], array[fkeys_d.reloid::regclass::text, fkeys_d.conkey]))
             WHEN COUNT(fkeys_r.reloid) > 0
             THEN json_agg(json_object(array['table', 'column'], array[fkeys_r.freloid::regclass::text, fkeys_r.confkey]))
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
     where relkind = 'r' and c.relpersistence = 'p'
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
    group by 1,2,3
    order by citus_table_type, c.oid
)
SELECT conrelid, confrelid
FROM initial_suggestions t1
JOIN pg_constraint ON (conrelid = t1.table_oid)
WHERE confrelid != 0 AND
      -- find illegal conditions
      t1.citus_table_type = 'reference' AND
      EXISTS (SELECT 1 FROM initial_suggestions t2 WHERE t2.citus_table_type = 'distributed' AND t2.table_oid = confrelid);
