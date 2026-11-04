/* citus_observer--1.0.sql */

\echo Use "CREATE EXTENSION citus_observer" to load this file. \quit

/*
 * Every function reads the shared memory of the node it runs on. To see the
 * whole cluster, call them through run_command_on_all_nodes() from the
 * coordinator. Cumulative counters only grow until reset(); consumers
 * compute deltas between polls.
 */

/*
 * status reports, per instrumentation area, whether it is enabled, how many
 * entries its shared table holds, its capacity, and how many new entries were
 * dropped because the table was full.
 */
CREATE FUNCTION status(
    OUT area text,
    OUT enabled boolean,
    OUT entries bigint,
    OUT capacity bigint,
    OUT dropped bigint,
    OUT stats_reset timestamptz)
RETURNS SETOF record
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', 'citus_observer_status';

/* reset clears one area ('task', 'resource', 'plan', 'key', 'txn', 'contention') or all of them (NULL) */
CREATE FUNCTION reset(area text DEFAULT NULL)
RETURNS void
LANGUAGE C
AS 'MODULE_PATHNAME', 'citus_observer_reset';

/*
 * task_stats: one row per (shard, node, command) for tasks this node sent
 * to shard placements as a coordinator, including tasks it executed locally.
 * Times are measured from sending the task to receiving its last result.
 */
CREATE FUNCTION task_stats(
    OUT shardid bigint,
    OUT nodeid integer,
    OUT colocation_id integer,
    OUT command text,
    OUT calls bigint,
    OUT failures bigint,
    OUT local_calls bigint,
    OUT total_time_ms double precision,
    OUT max_time_ms double precision,
    OUT rows bigint,
    OUT bytes bigint)
RETURNS SETOF record
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', 'citus_observer_task_stats';

/*
 * shard_resource_stats: resources used on this node by statements that read
 * or wrote local shard relations. A statement that touches several shards
 * has its cost split evenly between them.
 */
CREATE FUNCTION shard_resource_stats(
    OUT shardid bigint,
    OUT calls bigint,
    OUT write_calls bigint,
    OUT exec_time_ms double precision,
    OUT cpu_user_ms double precision,
    OUT cpu_sys_ms double precision,
    OUT shared_blks_hit bigint,
    OUT shared_blks_read bigint,
    OUT shared_blks_dirtied bigint,
    OUT shared_blks_written bigint,
    OUT temp_blks_written bigint,
    OUT wal_records bigint,
    OUT wal_bytes bigint,
    OUT rows bigint)
RETURNS SETOF record
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', 'citus_observer_shard_resource_stats';

/*
 * query_plans: one row per distributed query fingerprint (queryid, same as
 * pg_stat_statements) planned on this node.
 *
 * plan_kind is one of: fast_path_router, router, multi_shard, repartition,
 * insert_select_pushdown, insert_select_repartition, insert_select_coordinator,
 * merge, recursive.
 *
 * reason is why the query could not be planned as a single-shard router
 * query, as reported by the router planner, or NULL if it was routable.
 */
CREATE FUNCTION query_plans(
    OUT queryid bigint,
    OUT plan_kind text,
    OUT reason text,
    OUT colocation_id integer,
    OUT calls bigint,
    OUT single_shard_calls bigint,
    OUT multi_shard_calls bigint,
    OUT total_tasks bigint,
    OUT max_tasks integer,
    OUT subplans integer,
    OUT total_time_ms double precision)
RETURNS SETOF record
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', 'citus_observer_query_plans';

/*
 * column_usage: how each distributed query fingerprint uses columns of
 * distributed tables. usage is one of: eq (column = constant/parameter),
 * in (column = ANY / IN list), range (<, <=, >, >=, BETWEEN),
 * join (column = column of another table), group (GROUP BY).
 */
CREATE FUNCTION column_usage(
    OUT queryid bigint,
    OUT table_name regclass,
    OUT column_name text,
    OUT usage text,
    OUT is_distribution_column boolean,
    OUT occurrences integer)
RETURNS SETOF record
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', 'citus_observer_column_usage';

/*
 * join_edges: equi-join conditions between columns of two distributed or
 * reference tables. pushdown is true when the join can run shard-local:
 * both sides are distribution columns of colocated tables, or one side is
 * a reference table.
 */
CREATE FUNCTION join_edges(
    OUT queryid bigint,
    OUT left_table regclass,
    OUT left_column text,
    OUT right_table regclass,
    OUT right_column text,
    OUT colocated boolean,
    OUT pushdown boolean)
RETURNS SETOF record
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', 'citus_observer_join_edges';

/* column usage weighted by how often each query ran */
CREATE VIEW column_usage_weighted AS
SELECT u.table_name, u.column_name, u.usage, u.is_distribution_column,
       sum(p.calls * u.occurrences)::bigint AS weighted_calls,
       count(DISTINCT u.queryid) AS queries
FROM column_usage() u
JOIN query_plans() p USING (queryid)
GROUP BY 1, 2, 3, 4;

/*
 * hot_keys: the most frequently accessed primary key values per shard,
 * seen by index lookups on this node's shards. count is an upper bound;
 * count - error is a lower bound. With reset => true the sketch is cleared
 * after reading, so each call covers the interval since the previous one.
 */
CREATE FUNCTION hot_keys(
    reset boolean DEFAULT false,
    OUT shardid bigint,
    OUT access text,
    OUT key text,
    OUT count bigint,
    OUT error bigint,
    OUT total bigint)
RETURNS SETOF record
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', 'citus_observer_hot_keys';

/*
 * txn_stats: counters for distributed transactions coordinated by this node.
 * multi_shard_group counts transactions whose single-shard statements
 * touched more than one shard group.
 */
CREATE FUNCTION txn_stats(
    OUT txns bigint,
    OUT multi_node bigint,
    OUT multi_shard_group bigint,
    OUT two_pc bigint,
    OUT aborted bigint,
    OUT prepare_time_ms double precision,
    OUT commit_time_ms double precision,
    OUT stats_reset timestamptz)
RETURNS SETOF record
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', 'citus_observer_txn_stats';

/*
 * shard_group_coaccess: how often two shard groups of the same colocation
 * group (identified by shard index) were read or written by single-shard
 * statements of the same distributed transaction. Statements that fan out
 * to many shards are not counted here (see fan_out_tasks in txn events).
 * shard_index_a < shard_index_b.
 */
CREATE FUNCTION shard_group_coaccess(
    OUT colocation_id integer,
    OUT shard_index_a integer,
    OUT shard_index_b integer,
    OUT txns bigint,
    OUT two_pc bigint)
RETURNS SETOF record
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', 'citus_observer_shard_group_coaccess';

/*
 * tenant_coaccess: how often two distribution column values (tenants) were
 * accessed by single-shard statements of the same distributed transaction.
 * tenant_a < tenant_b by text comparison.
 */
CREATE FUNCTION tenant_coaccess(
    OUT colocation_id integer,
    OUT tenant_a text,
    OUT tenant_b text,
    OUT txns bigint)
RETURNS SETOF record
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', 'citus_observer_tenant_coaccess';

/*
 * contention: lock waits on this node, sampled by a background worker every
 * citus_observer.contention_interval_ms. wait_ms is samples times the
 * interval. key is the primary key of the contended row (e.g.
 * 'tenant_id=7,product_id=42') when the wait is on a row, NULL otherwise.
 */
CREATE FUNCTION contention(
    OUT shardid bigint,
    OUT relation regclass,
    OUT locktype text,
    OUT mode text,
    OUT key text,
    OUT samples bigint,
    OUT wait_ms double precision)
RETURNS SETOF record
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', 'citus_observer_contention';

/*
 * events: discrete events with seq > since, oldest first. kind is one of
 * txn, action_phase, action_end, or gap (events after since were
 * overwritten before being read; payload has {"lost": n}).
 */
CREATE FUNCTION events(
    since bigint DEFAULT 0,
    max_events integer DEFAULT 10000,
    OUT seq bigint,
    OUT ts timestamptz,
    OUT kind text,
    OUT payload jsonb)
RETURNS SETOF record
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', 'citus_observer_events';

/*
 * topology_version: a hash of node, shard and placement metadata. It changes
 * whenever a shard is moved, split, created or dropped, or a node changes.
 */
CREATE FUNCTION topology_version()
RETURNS text
LANGUAGE sql STABLE
AS $$
  SELECT md5(coalesce(string_agg(x, ';' ORDER BY x), ''))
  FROM (
    SELECT format('n%s:%s:%s:%s:%s', nodeid, nodename, nodeport, isactive, shouldhaveshards)
    FROM pg_catalog.pg_dist_node
    UNION ALL
    SELECT format('s%s:%s:%s:%s', shardid, logicalrelid::oid, shardminvalue, shardmaxvalue)
    FROM pg_catalog.pg_dist_shard
    UNION ALL
    SELECT format('p%s:%s', shardid, groupid)
    FROM pg_catalog.pg_dist_placement
  ) t(x)
$$;

/*
 * Change capture: a logical decoding slot with the citus_observer_decoder
 * plugin summarizes inserted, updated and deleted rows per shard and
 * distribution column value, straight from the WAL. Changes Citus itself
 * makes while moving or splitting shards are excluded.
 */
CREATE FUNCTION enable_change_capture(slot_name name DEFAULT 'citus_observer')
RETURNS boolean
LANGUAGE plpgsql
AS $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_replication_slots s WHERE s.slot_name = enable_change_capture.slot_name) THEN
    RETURN false;
  END IF;
  PERFORM pg_catalog.pg_create_logical_replication_slot(slot_name, 'citus_observer_decoder');
  RETURN true;
END;
$$;

CREATE FUNCTION disable_change_capture(slot_name name DEFAULT 'citus_observer')
RETURNS boolean
LANGUAGE plpgsql
AS $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_replication_slots s WHERE s.slot_name = disable_change_capture.slot_name) THEN
    RETURN false;
  END IF;
  PERFORM pg_catalog.pg_drop_replication_slot(slot_name);
  RETURN true;
END;
$$;

/*
 * distribution_columns: for each shard of a hash-distributed table placed on
 * this node, the local shard relation and the attribute number of its
 * distribution column. The decoder receives this map as an option, since
 * Citus metadata cannot be read while decoding.
 */
CREATE FUNCTION distribution_columns(
    OUT shardid bigint,
    OUT shard_relation regclass,
    OUT attnum smallint)
RETURNS SETOF record
LANGUAGE sql STABLE
AS $$
  SELECT s.shardid, r.oid::regclass, a.attnum
  FROM pg_catalog.pg_dist_shard s
  JOIN pg_catalog.pg_dist_partition p USING (logicalrelid)
  CROSS JOIN LATERAL pg_catalog.to_regclass(
      pg_catalog.shard_name(s.logicalrelid, s.shardid, false)) r(oid)
  JOIN pg_catalog.pg_attribute a
    ON a.attrelid = r.oid
   AND a.attname = pg_catalog.column_to_column_name(s.logicalrelid, p.partkey)
  WHERE p.partmethod = 'h' AND r.oid IS NOT NULL
$$;

/*
 * change_summary consumes the change capture slot and returns, per shard and
 * distribution column value, the rows inserted, updated and deleted, the
 * tuple bytes written, and the number of transactions, since the previous
 * call. upto_changes limits how many WAL changes are decoded in one call.
 */
CREATE FUNCTION change_summary(
    slot_name name DEFAULT 'citus_observer',
    upto_changes integer DEFAULT NULL,
    OUT shardid bigint,
    OUT dist_value text,
    OUT inserts bigint,
    OUT updates bigint,
    OUT deletes bigint,
    OUT bytes bigint,
    OUT txns bigint)
RETURNS SETOF record
LANGUAGE sql
AS $$
  WITH map AS (
    SELECT coalesce(string_agg(format('%s:%s:%s', shard_relation::oid, attnum, d.shardid), ';'), '') AS m
    FROM citus_observer.distribution_columns() d
  ),
  raw AS (
    SELECT pg_catalog.string_to_array(c.data, E'\t') AS f
    FROM map, pg_catalog.pg_logical_slot_get_changes(
        change_summary.slot_name, NULL, change_summary.upto_changes,
        'dist_columns', map.m) c
  )
  SELECT f[1]::bigint, nullif(f[2], '\N'),
         sum(f[3]::bigint)::bigint, sum(f[4]::bigint)::bigint, sum(f[5]::bigint)::bigint,
         sum(f[6]::bigint)::bigint, count(*)
  FROM raw
  GROUP BY 1, 2
$$;
