// Copyright (c) 2026 Eric Chubb
// Licensed under the MIT License

use crate::registry::{self, RootKey, RegistryValue};
use rusqlite::{params, Connection};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

// ── Public types returned by searches ───────────────────────────────────────

#[derive(Debug, Clone)]
pub struct IndexedKey {
    pub root: RootKey,
    pub path: String,
    pub name_lower: String,
}

#[derive(Debug, Clone)]
pub struct IndexedValue {
    pub root: RootKey,
    pub key_path: String,
    pub value_name: String,
    pub value_name_lower: String,
    pub value_data_text: String,
    pub value_type: String,
}

// ── Index stats (for UI) ────────────────────────────────────────────────────

#[derive(Debug, Clone, Default)]
pub struct IndexStats {
    pub key_count: u64,
    pub value_count: u64,
    pub last_sync: Option<Instant>,
}

// ── Shared state ────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct RegistryIndex {
    db_path: Arc<PathBuf>,
    pub is_indexing: Arc<AtomicBool>,
    pub cancel: Arc<AtomicBool>,
    pub keys_indexed: Arc<AtomicU64>,
    pub keys_skipped: Arc<AtomicU64>,
    pub current_path: Arc<Mutex<String>>,
    pub refresh_interval_secs: Arc<Mutex<u64>>,
    pub roots: Arc<Mutex<Vec<RootKey>>>,
    pub max_depth: Arc<Mutex<Option<usize>>>,
    pub enabled: Arc<AtomicBool>,
    pub index_values: Arc<AtomicBool>,
    pub stats: Arc<Mutex<IndexStats>>,
}

impl RegistryIndex {
    pub fn new() -> Self {
        let mut db_dir = dirs::config_dir().unwrap_or_else(|| PathBuf::from("."));
        db_dir.push("registry-editor");
        std::fs::create_dir_all(&db_dir).ok();
        let db_path = db_dir.join("index.db");

        let idx = Self {
            db_path: Arc::new(db_path),
            is_indexing: Arc::new(AtomicBool::new(false)),
            cancel: Arc::new(AtomicBool::new(false)),
            keys_indexed: Arc::new(AtomicU64::new(0)),
            keys_skipped: Arc::new(AtomicU64::new(0)),
            current_path: Arc::new(Mutex::new(String::new())),
            refresh_interval_secs: Arc::new(Mutex::new(300)),
            roots: Arc::new(Mutex::new(vec![
                RootKey::HkeyCurrentUser,
                RootKey::HkeyLocalMachine,
            ])),
            max_depth: Arc::new(Mutex::new(Some(8))),
            enabled: Arc::new(AtomicBool::new(true)),
            index_values: Arc::new(AtomicBool::new(true)),
            stats: Arc::new(Mutex::new(IndexStats::default())),
        };

        if let Ok(conn) = idx.open_db() {
            init_schema(&conn);
            idx.refresh_stats_from_db(&conn);
        }

        idx
    }

    pub(crate) fn open_db(&self) -> Result<Connection, rusqlite::Error> {
        let conn = Connection::open(self.db_path.as_ref())?;
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA cache_size = -8000;
             PRAGMA temp_store = MEMORY;",
        )?;
        Ok(conn)
    }

    fn refresh_stats_from_db(&self, conn: &Connection) {
        let key_count: u64 = conn
            .query_row("SELECT COUNT(*) FROM keys", [], |r| r.get(0))
            .unwrap_or(0);
        let value_count: u64 = conn
            .query_row("SELECT COUNT(*) FROM idx_values", [], |r| r.get(0))
            .unwrap_or(0);
        let mut stats = self.stats.lock().unwrap();
        stats.key_count = key_count;
        stats.value_count = value_count;
        if key_count > 0 {
            stats.last_sync = Some(Instant::now());
        }
    }

    pub fn has_index(&self) -> bool {
        self.stats.lock().unwrap().key_count > 0
    }

    pub fn age_secs(&self) -> Option<u64> {
        self.stats.lock().unwrap().last_sync.map(|t| t.elapsed().as_secs())
    }

    pub fn is_stale(&self) -> bool {
        let interval = *self.refresh_interval_secs.lock().unwrap();
        match self.age_secs() {
            Some(age) => age >= interval,
            None => true,
        }
    }

    // ── Cache read methods (used by RegistryCache) ──────────────────────────

    /// Get cached subkeys for a path (direct children only)
    pub fn get_cached_subkeys(&self, root: &RootKey, path: &str) -> Option<Vec<String>> {
        let conn = self.open_db().ok()?;
        let root_str = root.to_string();
        let mut stmt = conn
            .prepare(
                "SELECT path FROM keys WHERE root = ?1 AND parent_path = ?2 ORDER BY name_lower",
            )
            .ok()?;
        let results: Vec<String> = stmt
            .query_map(params![root_str, path], |row| row.get::<_, String>(0))
            .ok()?
            .filter_map(|r| r.ok())
            .map(|full_path| {
                full_path
                    .rsplit('\\')
                    .next()
                    .unwrap_or(&full_path)
                    .to_string()
            })
            .collect();
        if results.is_empty() {
            None // cache miss — could be empty or not cached
        } else {
            Some(results)
        }
    }

    /// Get cached last_write_time for a key
    pub fn get_cached_lwt(&self, root: &RootKey, path: &str) -> Option<u64> {
        let conn = self.open_db().ok()?;
        let root_str = root.to_string();
        conn.query_row(
            "SELECT last_write_time FROM keys WHERE root = ?1 AND path = ?2",
            params![root_str, path],
            |row| row.get(0),
        )
        .ok()
    }

    // ── Cache write methods (used by RegistryCache and sync) ────────────────

    /// Insert or update a single key in the index
    pub fn upsert_key(&self, root: &RootKey, path: &str, lwt: u64) {
        let conn = match self.open_db() {
            Ok(c) => c,
            Err(_) => return,
        };
        let root_str = root.to_string();
        let key_name = path.rsplit('\\').next().unwrap_or(path);
        let parent = parent_path(path);
        conn.execute(
            "INSERT OR REPLACE INTO keys (root, path, name_lower, parent_path, last_write_time) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![root_str, path, key_name.to_lowercase(), parent, lwt],
        )
        .ok();
    }

    /// Remove a key and its values from the index
    pub fn remove_key(&self, root: &RootKey, path: &str) {
        let conn = match self.open_db() {
            Ok(c) => c,
            Err(_) => return,
        };
        let root_str = root.to_string();
        // Remove the key and all descendants
        let like_pattern = format!("{}\\%", path.replace('%', "\\%").replace('_', "\\_"));
        conn.execute_batch("BEGIN").ok();
        conn.execute(
            "DELETE FROM keys WHERE root = ?1 AND (path = ?2 OR path LIKE ?3 ESCAPE '\\')",
            params![root_str, path, like_pattern],
        )
        .ok();
        conn.execute(
            "DELETE FROM idx_values WHERE root = ?1 AND (key_path = ?2 OR key_path LIKE ?3 ESCAPE '\\')",
            params![root_str, path, like_pattern],
        )
        .ok();
        conn.execute_batch("COMMIT").ok();
    }

    /// Update the indexed values for a key (delete old, insert new)
    pub fn upsert_values(&self, root: &RootKey, path: &str, values: &[RegistryValue]) {
        let conn = match self.open_db() {
            Ok(c) => c,
            Err(_) => return,
        };
        let root_str = root.to_string();
        conn.execute_batch("BEGIN").ok();
        conn.execute(
            "DELETE FROM idx_values WHERE root = ?1 AND key_path = ?2",
            params![root_str, path],
        )
        .ok();
        for val in values {
            let display_name = if val.name.is_empty() {
                "(Default)".to_string()
            } else {
                val.name.clone()
            };
            let data_text = val.data.searchable_text();
            let type_name = val.data.type_name();
            conn.execute(
                "INSERT INTO idx_values \
                 (root, key_path, value_name, value_name_lower, value_data_text, value_type) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![root_str, path, display_name, display_name.to_lowercase(), data_text, type_name],
            )
            .ok();
        }
        conn.execute_batch("COMMIT").ok();
    }

    /// Remove a single value from the index
    pub fn remove_value(&self, root: &RootKey, path: &str, value_name: &str) {
        let conn = match self.open_db() {
            Ok(c) => c,
            Err(_) => return,
        };
        let root_str = root.to_string();
        let display_name = if value_name.is_empty() { "(Default)" } else { value_name };
        conn.execute(
            "DELETE FROM idx_values WHERE root = ?1 AND key_path = ?2 AND value_name = ?3",
            params![root_str, path, display_name],
        )
        .ok();
    }

    // ── Bookmark methods ────────────────────────────────────────────────────

    pub fn get_bookmarks(&self) -> Vec<crate::bookmarks::Bookmark> {
        let conn = match self.open_db() {
            Ok(c) => c,
            Err(_) => return Vec::new(),
        };
        let mut stmt = conn
            .prepare("SELECT name, path, notes, color, sort_order FROM bookmarks ORDER BY sort_order")
            .unwrap();
        stmt.query_map([], |row| {
            Ok(crate::bookmarks::Bookmark {
                name: row.get(0)?,
                path: row.get(1)?,
                notes: row.get::<_, String>(2).unwrap_or_default(),
                color: row
                    .get::<_, Option<String>>(3)
                    .ok()
                    .flatten()
                    .and_then(|s| color_from_str(&s)),
            })
        })
        .unwrap()
        .filter_map(|r| r.ok())
        .collect()
    }

    pub fn add_bookmark(&self, bm: &crate::bookmarks::Bookmark) {
        let conn = match self.open_db() {
            Ok(c) => c,
            Err(_) => return,
        };
        let max_order: i64 = conn
            .query_row("SELECT COALESCE(MAX(sort_order), 0) FROM bookmarks", [], |r| r.get(0))
            .unwrap_or(0);
        let color_str = bm.color.as_ref().map(|c| c.name().to_string());
        conn.execute(
            "INSERT OR IGNORE INTO bookmarks (name, path, notes, color, sort_order) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![bm.name, bm.path, bm.notes, color_str, max_order + 1],
        )
        .ok();
    }

    pub fn remove_bookmark(&self, path: &str) {
        let conn = match self.open_db() {
            Ok(c) => c,
            Err(_) => return,
        };
        conn.execute("DELETE FROM bookmarks WHERE path = ?1", params![path]).ok();
    }

    pub fn update_bookmark(&self, path: &str, bm: &crate::bookmarks::Bookmark) {
        let conn = match self.open_db() {
            Ok(c) => c,
            Err(_) => return,
        };
        let color_str = bm.color.as_ref().map(|c| c.name().to_string());
        conn.execute(
            "UPDATE bookmarks SET name = ?1, notes = ?2, color = ?3 WHERE path = ?4",
            params![bm.name, bm.notes, color_str, path],
        )
        .ok();
    }

    pub fn is_bookmarked(&self, path: &str) -> bool {
        let conn = match self.open_db() {
            Ok(c) => c,
            Err(_) => return false,
        };
        conn.query_row(
            "SELECT COUNT(*) FROM bookmarks WHERE path = ?1",
            params![path],
            |r| r.get::<_, i64>(0),
        )
        .unwrap_or(0)
            > 0
    }

    pub fn move_bookmark(&self, path: &str, direction: i32) {
        let conn = match self.open_db() {
            Ok(c) => c,
            Err(_) => return,
        };
        // Get current sort_order
        let current_order: i64 = match conn.query_row(
            "SELECT sort_order FROM bookmarks WHERE path = ?1",
            params![path],
            |r| r.get(0),
        ) {
            Ok(o) => o,
            Err(_) => return,
        };

        let swap_order = current_order + direction as i64;

        // Find the bookmark at the swap position
        let swap_path: Option<String> = conn
            .query_row(
                "SELECT path FROM bookmarks WHERE sort_order = ?1",
                params![swap_order],
                |r| r.get(0),
            )
            .ok();

        if let Some(sp) = swap_path {
            conn.execute_batch("BEGIN").ok();
            conn.execute(
                "UPDATE bookmarks SET sort_order = ?1 WHERE path = ?2",
                params![swap_order, path],
            )
            .ok();
            conn.execute(
                "UPDATE bookmarks SET sort_order = ?1 WHERE path = ?2",
                params![current_order, sp],
            )
            .ok();
            conn.execute_batch("COMMIT").ok();
        }
    }

    // ── Search methods ──────────────────────────────────────────────────────

    pub fn search_keys(&self, query: &str, case_sensitive: bool, use_regex: bool) -> Vec<IndexedKey> {
        let conn = match self.open_db() {
            Ok(c) => c,
            Err(_) => return Vec::new(),
        };

        if use_regex {
            let pattern = if case_sensitive {
                query.to_string()
            } else {
                format!("(?i){}", query)
            };
            let re = match regex::Regex::new(&pattern) {
                Ok(r) => r,
                Err(_) => return Vec::new(),
            };
            let mut stmt = conn.prepare("SELECT root, path, name_lower FROM keys").unwrap();
            stmt.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?))
            })
            .unwrap()
            .filter_map(|r| r.ok())
            .filter(|(_, path, _)| {
                let name = path.rsplit('\\').next().unwrap_or(path);
                re.is_match(name)
            })
            .map(|(root_str, path, name_lower)| IndexedKey {
                root: RootKey::from_name(&root_str).unwrap_or(RootKey::HkeyCurrentUser),
                path,
                name_lower,
            })
            .collect()
        } else {
            let like = format!(
                "%{}%",
                query.to_lowercase().replace('%', "\\%").replace('_', "\\_")
            );
            let mut stmt = conn
                .prepare("SELECT root, path, name_lower FROM keys WHERE name_lower LIKE ?1 ESCAPE '\\'")
                .unwrap();
            stmt.query_map(params![like], |row| {
                Ok(IndexedKey {
                    root: RootKey::from_name(&row.get::<_, String>(0)?).unwrap_or(RootKey::HkeyCurrentUser),
                    path: row.get(1)?,
                    name_lower: row.get(2)?,
                })
            })
            .unwrap()
            .filter_map(|r| r.ok())
            .filter(|k| {
                if case_sensitive {
                    let name = k.path.rsplit('\\').next().unwrap_or(&k.path);
                    name.contains(query)
                } else {
                    true
                }
            })
            .collect()
        }
    }

    pub fn search_values(
        &self,
        query: &str,
        case_sensitive: bool,
        use_regex: bool,
        search_names: bool,
        search_data: bool,
        type_filter: Option<&str>,
    ) -> Vec<IndexedValue> {
        let conn = match self.open_db() {
            Ok(c) => c,
            Err(_) => return Vec::new(),
        };

        if use_regex {
            let pattern = if case_sensitive {
                query.to_string()
            } else {
                format!("(?i){}", query)
            };
            let re = match regex::Regex::new(&pattern) {
                Ok(r) => r,
                Err(_) => return Vec::new(),
            };
            let sql = if let Some(tf) = type_filter {
                format!(
                    "SELECT root, key_path, value_name, value_name_lower, value_data_text, value_type \
                     FROM idx_values WHERE value_type = '{}'",
                    tf.replace('\'', "''")
                )
            } else {
                "SELECT root, key_path, value_name, value_name_lower, value_data_text, value_type FROM idx_values".to_string()
            };
            let mut stmt = conn.prepare(&sql).unwrap();
            stmt.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?, row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?, row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?, row.get::<_, String>(5)?,
                ))
            })
            .unwrap()
            .filter_map(|r| r.ok())
            .filter(|(_, _, vname, _, vdata, _)| {
                (search_names && re.is_match(vname)) || (search_data && re.is_match(vdata))
            })
            .map(|(rs, kp, vn, vnl, vdt, vt)| IndexedValue {
                root: RootKey::from_name(&rs).unwrap_or(RootKey::HkeyCurrentUser),
                key_path: kp, value_name: vn, value_name_lower: vnl,
                value_data_text: vdt, value_type: vt,
            })
            .collect()
        } else {
            let like = format!(
                "%{}%",
                query.to_lowercase().replace('%', "\\%").replace('_', "\\_")
            );
            let mut conditions = Vec::new();
            if search_names {
                conditions.push("value_name_lower LIKE ?1 ESCAPE '\\'");
            }
            if search_data {
                conditions.push("LOWER(value_data_text) LIKE ?1 ESCAPE '\\'");
            }
            if conditions.is_empty() {
                return Vec::new();
            }
            let type_clause = type_filter
                .map(|tf| format!(" AND value_type = '{}'", tf.replace('\'', "''")))
                .unwrap_or_default();
            let sql = format!(
                "SELECT root, key_path, value_name, value_name_lower, value_data_text, value_type \
                 FROM idx_values WHERE ({}){}",
                conditions.join(" OR "),
                type_clause
            );
            let mut stmt = conn.prepare(&sql).unwrap();
            stmt.query_map(params![like], |row| {
                Ok(IndexedValue {
                    root: RootKey::from_name(&row.get::<_, String>(0)?).unwrap_or(RootKey::HkeyCurrentUser),
                    key_path: row.get(1)?, value_name: row.get(2)?,
                    value_name_lower: row.get(3)?, value_data_text: row.get(4)?,
                    value_type: row.get(5)?,
                })
            })
            .unwrap()
            .filter_map(|r| r.ok())
            .collect()
        }
    }

    // ── Background sync loop ────────────────────────────────────────────────

    pub fn start_background_loop(&self) {
        let index = self.clone();
        std::thread::spawn(move || {
            sync_registry_to_db(&index);
            loop {
                if !index.enabled.load(Ordering::Relaxed) {
                    std::thread::sleep(Duration::from_secs(5));
                    continue;
                }
                if index.is_stale() && !index.is_indexing.load(Ordering::Relaxed) {
                    sync_registry_to_db(&index);
                }
                std::thread::sleep(Duration::from_secs(5));
            }
        });
    }

    pub fn rebuild_now(&self) {
        if self.is_indexing.load(Ordering::Relaxed) {
            self.cancel.store(true, Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(100));
        }
        let index = self.clone();
        std::thread::spawn(move || {
            sync_registry_to_db(&index);
        });
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────────

fn parent_path(path: &str) -> String {
    match path.rfind('\\') {
        Some(pos) => path[..pos].to_string(),
        None => String::new(), // direct child of root
    }
}

fn color_from_str(s: &str) -> Option<crate::bookmarks::BookmarkColor> {
    match s {
        "Red" => Some(crate::bookmarks::BookmarkColor::Red),
        "Green" => Some(crate::bookmarks::BookmarkColor::Green),
        "Blue" => Some(crate::bookmarks::BookmarkColor::Blue),
        "Yellow" => Some(crate::bookmarks::BookmarkColor::Yellow),
        "Purple" => Some(crate::bookmarks::BookmarkColor::Purple),
        "Orange" => Some(crate::bookmarks::BookmarkColor::Orange),
        _ => None,
    }
}

// ── Schema ──────────────────────────────────────────────────────────────────

fn init_schema(conn: &Connection) {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS keys (
            root            TEXT NOT NULL,
            path            TEXT NOT NULL,
            name_lower      TEXT NOT NULL,
            parent_path     TEXT NOT NULL DEFAULT '',
            last_write_time INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (root, path)
        );

        CREATE TABLE IF NOT EXISTS idx_values (
            root             TEXT NOT NULL,
            key_path         TEXT NOT NULL,
            value_name       TEXT NOT NULL,
            value_name_lower TEXT NOT NULL,
            value_data_text  TEXT NOT NULL DEFAULT '',
            value_type       TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (root, key_path, value_name)
        );

        CREATE TABLE IF NOT EXISTS bookmarks (
            path       TEXT PRIMARY KEY,
            name       TEXT NOT NULL,
            notes      TEXT NOT NULL DEFAULT '',
            color      TEXT,
            sort_order INTEGER NOT NULL DEFAULT 0
        );

        CREATE INDEX IF NOT EXISTS idx_keys_name_lower ON keys(name_lower);
        CREATE INDEX IF NOT EXISTS idx_keys_parent ON keys(root, parent_path);
        CREATE INDEX IF NOT EXISTS idx_val_name_lower ON idx_values(value_name_lower);
        CREATE INDEX IF NOT EXISTS idx_val_key ON idx_values(root, key_path);",
    )
    .ok();

    // Migration: add parent_path if missing (from older schema)
    conn.execute_batch(
        "ALTER TABLE keys ADD COLUMN parent_path TEXT NOT NULL DEFAULT ''",
    )
    .ok(); // silently fails if column already exists
}

// ── Registry → DB sync ─────────────────────────────────────────────────────

fn sync_registry_to_db(index: &RegistryIndex) {
    index.is_indexing.store(true, Ordering::SeqCst);
    index.cancel.store(false, Ordering::SeqCst);
    index.keys_indexed.store(0, Ordering::SeqCst);
    index.keys_skipped.store(0, Ordering::SeqCst);
    *index.current_path.lock().unwrap() = String::new();

    let conn = match index.open_db() {
        Ok(c) => c,
        Err(_) => {
            index.is_indexing.store(false, Ordering::SeqCst);
            return;
        }
    };

    let roots = index.roots.lock().unwrap().clone();
    let max_depth = *index.max_depth.lock().unwrap();
    let do_values = index.index_values.load(Ordering::Relaxed);

    // Load existing timestamps for fast comparison
    let mut db_timestamps: std::collections::HashMap<String, u64> =
        std::collections::HashMap::new();
    {
        let mut stmt = conn
            .prepare("SELECT root, path, last_write_time FROM keys")
            .unwrap();
        for row in stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, u64>(2)?,
                ))
            })
            .unwrap()
            .flatten()
        {
            db_timestamps.insert(format!("{}\\{}", row.0, row.1), row.2);
        }
    }

    let mut visited: HashSet<String> = HashSet::new();

    conn.execute("BEGIN", []).ok();

    for root in &roots {
        if index.cancel.load(Ordering::Relaxed) {
            break;
        }
        walk_and_sync(root, "", max_depth, do_values, index, &conn, &db_timestamps, &mut visited, 0);
    }

    if !index.cancel.load(Ordering::Relaxed) {
        // Delete keys that no longer exist
        let root_names: Vec<String> = roots.iter().map(|r| r.to_string()).collect();
        for root_name in &root_names {
            let stale_keys: Vec<String> = {
                let mut stmt = conn
                    .prepare("SELECT path FROM keys WHERE root = ?1")
                    .unwrap();
                stmt.query_map(params![root_name], |row| row.get::<_, String>(0))
                    .unwrap()
                    .filter_map(|r| r.ok())
                    .filter(|p| !visited.contains(&format!("{}\\{}", root_name, p)))
                    .collect()
            };
            for path in &stale_keys {
                conn.execute("DELETE FROM keys WHERE root = ?1 AND path = ?2", params![root_name, path]).ok();
                conn.execute("DELETE FROM idx_values WHERE root = ?1 AND key_path = ?2", params![root_name, path]).ok();
            }
        }

        conn.execute("COMMIT", []).ok();
        index.refresh_stats_from_db(&conn);
    } else {
        conn.execute("ROLLBACK", []).ok();
    }

    index.is_indexing.store(false, Ordering::SeqCst);
}

fn walk_and_sync(
    root: &RootKey,
    path: &str,
    max_depth: Option<usize>,
    do_values: bool,
    index: &RegistryIndex,
    conn: &Connection,
    db_timestamps: &std::collections::HashMap<String, u64>,
    visited: &mut HashSet<String>,
    depth: usize,
) {
    if index.cancel.load(Ordering::Relaxed) {
        return;
    }
    if let Some(md) = max_depth {
        if depth > md {
            return;
        }
    }

    let root_str = root.to_string();
    let full_key = format!("{}\\{}", root_str, path);
    visited.insert(full_key.clone());

    index.keys_indexed.fetch_add(1, Ordering::Relaxed);
    if index.keys_indexed.load(Ordering::Relaxed) % 500 == 0 {
        if let Ok(mut cp) = index.current_path.lock() {
            *cp = full_key.clone();
        }
    }

    if !path.is_empty() {
        let current_lwt = registry::get_last_write_time(root, path).unwrap_or(0);
        let db_lwt = db_timestamps.get(&full_key).copied().unwrap_or(u64::MAX);

        if current_lwt == db_lwt && db_lwt != u64::MAX {
            index.keys_skipped.fetch_add(1, Ordering::Relaxed);
        } else {
            let key_name = path.rsplit('\\').next().unwrap_or(path);
            let parent = parent_path(path);
            conn.execute(
                "INSERT OR REPLACE INTO keys (root, path, name_lower, parent_path, last_write_time) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![root_str, path, key_name.to_lowercase(), parent, current_lwt],
            )
            .ok();

            if do_values {
                conn.execute(
                    "DELETE FROM idx_values WHERE root = ?1 AND key_path = ?2",
                    params![root_str, path],
                )
                .ok();
                if let Ok(vals) = registry::enumerate_values(root, path) {
                    for val in vals {
                        let display_name = if val.name.is_empty() {
                            "(Default)".to_string()
                        } else {
                            val.name.clone()
                        };
                        let data_text = val.data.searchable_text();
                        let type_name = val.data.type_name();
                        conn.execute(
                            "INSERT INTO idx_values \
                             (root, key_path, value_name, value_name_lower, value_data_text, value_type) \
                             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                            params![root_str, path, display_name, display_name.to_lowercase(), data_text, type_name],
                        )
                        .ok();
                    }
                }
            }
        }
    }

    if let Ok(subkeys) = registry::enumerate_subkeys(root, path) {
        for subkey in subkeys {
            let child_path = if path.is_empty() { subkey } else { format!("{}\\{}", path, subkey) };
            walk_and_sync(root, &child_path, max_depth, do_values, index, conn, db_timestamps, visited, depth + 1);
        }
    }
}
