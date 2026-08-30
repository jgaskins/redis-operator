//! Parsing of Redis `INFO` output and the catalogue of fields exported as
//! metrics.
//!
//! Everything here is pure text handling — no I/O, no Kubernetes — which is
//! what makes the interesting half of the exporter unit-testable against the
//! fixtures in `testdata/`.

use std::collections::BTreeMap;

/// One `INFO` reply, flattened.
///
/// Section headers are dropped rather than nested. INFO field names are unique
/// across sections, every consumer here looks a field up by name, and flattening
/// means one lookup path works for a standalone server, a cluster node, and a
/// sentinel alike — the sections they emit differ, but the field names don't.
#[derive(Debug, Clone, Default)]
pub struct Info {
    fields: BTreeMap<String, String>,
}

impl Info {
    /// Parse an `INFO` reply. Comment lines (`# Server`) and blank lines are
    /// skipped; anything else is split on the first `:`.
    ///
    /// Malformed lines are ignored rather than rejected. A reply that gained a
    /// line this parser doesn't understand must not cost us the ~50 fields on
    /// either side of it.
    pub fn parse(text: &str) -> Self {
        let mut fields = BTreeMap::new();
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Some((key, value)) = line.split_once(':') {
                fields.insert(key.to_string(), value.to_string());
            }
        }
        Self { fields }
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.fields.get(key).map(String::as_str)
    }

    /// A field's value as a number. `None` when the field is absent or isn't
    /// numeric — `used_memory_human:261.68M` and `maxmemory_policy:noeviction`
    /// are both fields the catalogue never asks for, but a future Redis could
    /// turn a numeric field into a word, and that should drop one series rather
    /// than poison the scrape.
    pub fn num(&self, key: &str) -> Option<f64> {
        self.get(key)?.trim().parse().ok()
    }

    /// True when a field holds the given word — the shape of the `ok`/`up`
    /// health fields (`rdb_last_bgsave_status`, `master_link_status`).
    pub fn is(&self, key: &str, value: &str) -> Option<bool> {
        Some(self.get(key)?.trim() == value)
    }

    /// Per-database key counts from the `# Keyspace` section.
    ///
    /// `db0:keys=110868,expires=109950,avg_ttl=0,subexpiry=0`
    pub fn keyspace(&self) -> Vec<KeyspaceDb> {
        let mut dbs = Vec::new();
        for (key, value) in &self.fields {
            let Some(index) = key.strip_prefix("db") else {
                continue;
            };
            // `db0_distrib_strings_sizes` also starts with `db` — the Keysizes
            // section, which is deliberately not exported (one field per size
            // bucket per type per db is unbounded cardinality).
            if !index.chars().all(|c| c.is_ascii_digit()) || index.is_empty() {
                continue;
            }
            let kv = parse_kv_list(value);
            dbs.push(KeyspaceDb {
                db: index.to_string(),
                keys: kv.get("keys").and_then(|v| v.parse().ok()).unwrap_or(0),
                expires: kv.get("expires").and_then(|v| v.parse().ok()).unwrap_or(0),
            });
        }
        dbs
    }

    /// Monitored masters from a sentinel's `# Sentinel` section.
    ///
    /// `master0:name=mymaster,status=ok,address=10.244.1.37:6379,slaves=2,sentinels=3`
    pub fn sentinel_masters(&self) -> Vec<SentinelMaster> {
        let mut masters = Vec::new();
        for (key, value) in &self.fields {
            let Some(index) = key.strip_prefix("master") else {
                continue;
            };
            // Excludes `master_host`, `master_link_status`, and the rest of the
            // replication fields, which share the prefix but not the shape.
            if index.is_empty() || !index.chars().all(|c| c.is_ascii_digit()) {
                continue;
            }
            let kv = parse_kv_list(value);
            let Some(name) = kv.get("name") else { continue };
            masters.push(SentinelMaster {
                name: name.to_string(),
                ok: kv.get("status").map(|s| *s == "ok").unwrap_or(false),
                slaves: kv.get("slaves").and_then(|v| v.parse().ok()).unwrap_or(0),
                sentinels: kv.get("sentinels").and_then(|v| v.parse().ok()).unwrap_or(0),
            });
        }
        masters
    }
}

/// Split a `k=v,k=v` field value. Values containing `=` (an `address=host:port`
/// never does, but `options=[a|b]` in the Modules section might) keep everything
/// after the first `=`.
fn parse_kv_list(s: &str) -> BTreeMap<&str, &str> {
    s.split(',').filter_map(|pair| pair.split_once('=')).collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyspaceDb {
    /// Database index as a string, ready to be used as a label value.
    pub db: String,
    pub keys: u64,
    pub expires: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SentinelMaster {
    pub name: String,
    pub ok: bool,
    pub slaves: u64,
    pub sentinels: u64,
}

/// Gauge or counter. The distinction is the whole reason for a hand-maintained
/// catalogue: only a correctly typed counter gets `_total`, and only a `_total`
/// series gets Prometheus's counter-reset handling in `rate()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Gauge,
    Counter,
}

/// An `INFO` field exported as a metric.
///
/// `name` carries no `_total` suffix even for counters: `prometheus-client`
/// appends the OpenMetrics suffix itself, from the metric type, and appending it
/// here would produce `..._total_total`.
#[derive(Debug, Clone, Copy)]
pub struct InfoMetric {
    pub key: &'static str,
    pub name: &'static str,
    pub help: &'static str,
    pub kind: Kind,
}

const fn gauge(key: &'static str, name: &'static str, help: &'static str) -> InfoMetric {
    InfoMetric { key, name, help, kind: Kind::Gauge }
}

const fn counter(key: &'static str, name: &'static str, help: &'static str) -> InfoMetric {
    InfoMetric { key, name, help, kind: Kind::Counter }
}

/// The `INFO` fields exported for every instance.
///
/// An explicit allowlist, not "every numeric field". `INFO` carries roughly 200
/// numbers, and three families of them are unbounded per instance:
/// `db0_distrib_*` (one field per size bucket per type per database),
/// `errorstat_*` (one per error code the server has ever replied with), and
/// `io_thread_N`. Auto-export would let a workload grow the operator's series
/// count without limit, so new fields are added here deliberately.
///
/// Applied to sentinels too. A sentinel reports Server, Clients, Stats, and CPU
/// like any other Redis; the fields it doesn't report are simply absent, which
/// is what lets one catalogue serve all three kinds.
pub const REDIS_METRICS: &[InfoMetric] = &[
    // Server
    gauge("uptime_in_seconds", "redis_uptime_seconds", "Seconds since the server started"),
    // Clients
    gauge("connected_clients", "redis_connected_clients", "Client connections, excluding replicas"),
    gauge("blocked_clients", "redis_blocked_clients", "Clients blocked in a blocking call"),
    gauge("maxclients", "redis_maxclients", "Configured client connection limit"),
    // Memory
    gauge("used_memory", "redis_memory_used_bytes", "Bytes allocated by the allocator"),
    gauge("used_memory_rss", "redis_memory_rss_bytes", "Bytes resident in RAM as seen by the OS"),
    gauge("used_memory_peak", "redis_memory_peak_bytes", "Peak bytes allocated"),
    gauge("maxmemory", "redis_memory_max_bytes", "Configured maxmemory limit, 0 when unlimited"),
    gauge("mem_fragmentation_ratio", "redis_memory_fragmentation_ratio", "RSS over allocated memory"),
    // Persistence
    gauge("loading", "redis_loading", "1 while loading a dataset from disk"),
    gauge("rdb_changes_since_last_save", "redis_rdb_changes_since_last_save", "Writes since the last successful save"),
    gauge("rdb_last_save_time", "redis_rdb_last_save_timestamp_seconds", "Unix time of the last successful save"),
    gauge("rdb_bgsave_in_progress", "redis_rdb_bgsave_in_progress", "1 while a BGSAVE is running"),
    gauge("aof_enabled", "redis_aof_enabled", "1 when append-only logging is on"),
    gauge("aof_rewrite_in_progress", "redis_aof_rewrite_in_progress", "1 while an AOF rewrite is running"),
    gauge("aof_current_size", "redis_aof_current_size_bytes", "Current size of the append-only file"),
    // Stats
    counter("total_connections_received", "redis_connections_received", "Connections accepted"),
    counter("total_commands_processed", "redis_commands_processed", "Commands processed"),
    counter("total_net_input_bytes", "redis_net_input_bytes", "Bytes read from the network"),
    counter("total_net_output_bytes", "redis_net_output_bytes", "Bytes written to the network"),
    counter("rejected_connections", "redis_rejected_connections", "Connections rejected at the maxclients limit"),
    counter("expired_keys", "redis_expired_keys", "Keys removed because they expired"),
    counter("evicted_keys", "redis_evicted_keys", "Keys evicted under the maxmemory policy"),
    counter("keyspace_hits", "redis_keyspace_hits", "Lookups of keys that existed"),
    counter("keyspace_misses", "redis_keyspace_misses", "Lookups of keys that did not exist"),
    counter("total_error_replies", "redis_error_replies", "Error replies returned"),
    counter("sync_full", "redis_sync_full", "Full replication syncs served"),
    counter("sync_partial_ok", "redis_sync_partial_ok", "Partial resyncs served"),
    counter("sync_partial_err", "redis_sync_partial_err", "Partial resyncs that had to fall back to a full sync"),
    gauge("instantaneous_ops_per_sec", "redis_instantaneous_ops_per_sec", "Commands per second, as sampled by Redis"),
    gauge("latest_fork_usec", "redis_latest_fork_usec", "Microseconds the most recent fork blocked the server"),
    // Replication
    gauge("connected_slaves", "redis_connected_slaves", "Replicas currently connected to this instance"),
    gauge("master_repl_offset", "redis_master_repl_offset", "Replication offset of this instance"),
    gauge("slave_repl_offset", "redis_slave_repl_offset", "Replication offset this replica has processed"),
    gauge("master_last_io_seconds_ago", "redis_master_last_io_seconds_ago", "Seconds since the last interaction with the master"),
    gauge("master_sync_in_progress", "redis_master_sync_in_progress", "1 while this replica is syncing from its master"),
    gauge("repl_backlog_size", "redis_repl_backlog_size_bytes", "Configured size of the replication backlog"),
    gauge("repl_backlog_histlen", "redis_repl_backlog_histlen_bytes", "Bytes currently held in the replication backlog"),
    // CPU
    counter("used_cpu_sys", "redis_cpu_sys_seconds", "System CPU consumed by the server"),
    counter("used_cpu_user", "redis_cpu_user_seconds", "User CPU consumed by the server"),
    counter("used_cpu_sys_children", "redis_cpu_sys_children_seconds", "System CPU consumed by background children"),
    counter("used_cpu_user_children", "redis_cpu_user_children_seconds", "User CPU consumed by background children"),
    // Cluster
    gauge("cluster_enabled", "redis_cluster_enabled", "1 when the instance runs in cluster mode"),
];

/// Fields only a sentinel reports.
pub const SENTINEL_METRICS: &[InfoMetric] = &[
    gauge("sentinel_masters", "redis_sentinel_masters", "Masters this sentinel monitors"),
    gauge("sentinel_tilt", "redis_sentinel_tilt", "1 while the sentinel is in TILT mode"),
    gauge("sentinel_running_scripts", "redis_sentinel_running_scripts", "Notification scripts currently running"),
    gauge("sentinel_scripts_queue_length", "redis_sentinel_scripts_queue_length", "Notification scripts waiting to run"),
];

/// An `INFO` field whose value is a word, exported as 1 when it equals `ok_value`
/// and 0 otherwise. These are the health fields — the ones that say a bgsave
/// failed or a replica lost its master — and they are worth more than any
/// number in the reply.
#[derive(Debug, Clone, Copy)]
pub struct StatusMetric {
    pub key: &'static str,
    pub name: &'static str,
    pub help: &'static str,
    pub ok_value: &'static str,
}

pub const STATUS_METRICS: &[StatusMetric] = &[
    StatusMetric {
        key: "master_link_status",
        name: "redis_master_link_up",
        help: "1 when this replica's link to its master is up",
        ok_value: "up",
    },
    StatusMetric {
        key: "rdb_last_bgsave_status",
        name: "redis_rdb_last_bgsave_success",
        help: "1 when the last background save succeeded",
        ok_value: "ok",
    },
    StatusMetric {
        key: "aof_last_bgrewrite_status",
        name: "redis_aof_last_bgrewrite_success",
        help: "1 when the last AOF rewrite succeeded",
        ok_value: "ok",
    },
    StatusMetric {
        key: "aof_last_write_status",
        name: "redis_aof_last_write_success",
        help: "1 when the last AOF write succeeded",
        ok_value: "ok",
    },
];

#[cfg(test)]
mod tests {
    use super::*;

    const REPLICA: &str = include_str!("testdata/info_replica.txt");
    const SENTINEL: &str = include_str!("testdata/info_sentinel.txt");

    #[test]
    fn parse_skips_section_headers_and_blank_lines() {
        let info = Info::parse(REPLICA);
        assert_eq!(info.get("Server"), None);
        assert_eq!(info.get(""), None);
        assert_eq!(info.get("Replication"), None);
    }

    #[test]
    fn parse_reads_numeric_and_string_fields() {
        let info = Info::parse(REPLICA);
        assert_eq!(info.num("used_memory"), Some(274387912.0));
        assert_eq!(info.num("used_cpu_sys"), Some(3482.498639));
        assert_eq!(info.get("redis_version"), Some("8.10.0"));
        assert_eq!(info.get("role"), Some("slave"));
    }

    #[test]
    fn parse_keeps_values_containing_colons() {
        // `listener0:name=tcp,bind=*,bind=-::*,port=6379` splits on the FIRST
        // colon only.
        let info = Info::parse(REPLICA);
        assert_eq!(info.get("listener0"), Some("name=tcp,bind=*,bind=-::*,port=6379"));
    }

    #[test]
    fn num_returns_none_for_non_numeric_fields() {
        let info = Info::parse(REPLICA);
        assert_eq!(info.num("maxmemory_policy"), None);
        assert_eq!(info.num("used_memory_human"), None);
        assert_eq!(info.num("nonexistent_field"), None);
    }

    #[test]
    fn is_compares_status_words() {
        let info = Info::parse(REPLICA);
        assert_eq!(info.is("master_link_status", "up"), Some(true));
        assert_eq!(info.is("rdb_last_bgsave_status", "ok"), Some(true));
        // Absent on a master, and absence must stay distinguishable from false.
        assert_eq!(Info::default().is("master_link_status", "up"), None);
    }

    #[test]
    fn keyspace_extracts_keys_and_expires_per_db() {
        let dbs = Info::parse(REPLICA).keyspace();
        assert_eq!(
            dbs,
            vec![KeyspaceDb { db: "0".into(), keys: 110868, expires: 109950 }]
        );
    }

    #[test]
    fn keyspace_ignores_the_keysizes_section() {
        // `db0_distrib_strings_sizes` shares the `db` prefix but is a histogram
        // of unbounded width — exporting it would be a cardinality bomb.
        let dbs = Info::parse(REPLICA).keyspace();
        assert_eq!(dbs.len(), 1);
    }

    #[test]
    fn keyspace_is_empty_for_a_sentinel() {
        assert!(Info::parse(SENTINEL).keyspace().is_empty());
    }

    #[test]
    fn sentinel_masters_extracts_status_and_counts() {
        let masters = Info::parse(SENTINEL).sentinel_masters();
        assert_eq!(
            masters,
            vec![SentinelMaster {
                name: "mymaster".into(),
                ok: true,
                slaves: 2,
                sentinels: 3,
            }]
        );
    }

    #[test]
    fn sentinel_masters_ignores_replication_fields_sharing_the_prefix() {
        // `master_host`, `master_link_status`, `master_repl_offset` all start
        // with `master` on a plain replica.
        assert!(Info::parse(REPLICA).sentinel_masters().is_empty());
    }

    #[test]
    fn catalogue_names_are_valid_and_unique() {
        let mut seen = std::collections::HashSet::new();
        let names = REDIS_METRICS
            .iter()
            .chain(SENTINEL_METRICS)
            .map(|m| m.name)
            .chain(STATUS_METRICS.iter().map(|m| m.name));
        for name in names {
            assert!(
                name.starts_with("redis_")
                    && name
                        .chars()
                        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'),
                "invalid metric name {name:?}",
            );
            assert!(seen.insert(name), "duplicate metric name {name:?}");
        }
    }

    #[test]
    fn catalogue_counter_names_omit_the_total_suffix() {
        // prometheus-client appends `_total` from the metric type; spelling it
        // here too would produce `..._total_total`.
        for m in REDIS_METRICS.iter().chain(SENTINEL_METRICS) {
            assert!(
                !m.name.ends_with("_total"),
                "{:?} must not spell its own _total suffix",
                m.name
            );
        }
    }

    #[test]
    fn catalogue_excludes_human_readable_and_unbounded_fields() {
        for m in REDIS_METRICS.iter().chain(SENTINEL_METRICS) {
            assert!(!m.key.ends_with("_human"), "{:?} is a display string", m.key);
            assert!(!m.key.starts_with("errorstat_"), "{:?} is unbounded", m.key);
            assert!(!m.key.contains("_distrib_"), "{:?} is unbounded", m.key);
            assert!(!m.key.starts_with("io_thread_"), "{:?} is unbounded", m.key);
        }
    }

    #[test]
    fn every_catalogued_field_resolves_against_a_real_reply() {
        // Guards against typos in the catalogue: each entry must match at least
        // one of the two fixtures, or it is exporting nothing.
        let replica = Info::parse(REPLICA);
        let sentinel = Info::parse(SENTINEL);
        for m in REDIS_METRICS {
            assert!(
                replica.num(m.key).is_some() || sentinel.num(m.key).is_some(),
                "no fixture reports {:?}",
                m.key
            );
        }
        for m in SENTINEL_METRICS {
            assert!(sentinel.num(m.key).is_some(), "sentinel fixture lacks {:?}", m.key);
        }
        for m in STATUS_METRICS {
            assert!(replica.get(m.key).is_some(), "replica fixture lacks {:?}", m.key);
        }
    }
}
