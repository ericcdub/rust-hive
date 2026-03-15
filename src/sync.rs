// Copyright (c) 2026 Eric Chubb
// Licensed under the MIT License

//! # Sync Engine for SQLite-First Registry Editing
//!
//! This module implements a "SQLite-first" architecture for editing the Windows Registry.
//! Instead of reading/writing directly to the registry, we:
//!
//! 1. **Read from SQLite** - All UI reads come from our local database (the "working copy")
//! 2. **Write to SQLite first** - Changes are saved locally, marked as "pending"
//! 3. **Sync on demand** - User explicitly pushes changes to the registry or pulls updates
//!
//! ## Why SQLite-First?
//!
//! - **Safety**: Changes can be reviewed before applying to the actual registry
//! - **Speed**: SQLite queries are much faster than registry enumeration
//! - **Undo**: Pending changes can be discarded without affecting the registry
//! - **Offline editing**: Make changes even if registry access is restricted
//!
//! ## Architecture Overview
//!
//! ```text
//! ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
//! │   App UI    │────▶│   SQLite    │────▶│  Registry   │
//! │  (reads)    │     │  (cache)    │     │  (source)   │
//! └─────────────┘     └─────────────┘     └─────────────┘
//!       │                   ▲                   ▲
//!       │                   │                   │
//!       └───────────────────┴───────────────────┘
//!              writes go to SQLite first,
//!              then "push" syncs to registry
//! ```
//!
//! ## Key Concepts for Rust Beginners
//!
//! - `Arc<T>`: Atomic Reference Counted pointer - allows sharing data between threads safely
//! - `Mutex<T>`: Mutual exclusion lock - ensures only one thread accesses data at a time
//! - `AtomicBool/AtomicU64`: Thread-safe boolean/integer that can be read/written atomically
//! - `Clone`: A trait that allows creating a deep copy of a value

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// IMPORTS
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

// Import types from our own crate (this project)
use crate::bookmarks::{Bookmark, BookmarkColor};
use crate::registry::{self, RegValue, RegistryValue, RootKey};

// External crate imports:
// - rusqlite: SQLite database library for Rust
// - serde: Serialization/deserialization framework (for JSON storage)
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};

// Standard library imports:
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;             // Cross-platform file path handling
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU32, Ordering};  // Thread-safe primitives
use std::sync::{Arc, Mutex};        // Thread-safe smart pointers
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};  // Time handling

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// CHANGE TYPES - Representing pending modifications
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Represents a change that has been made in SQLite but not yet applied to the registry.
///
/// # Rust Concepts Explained
///
/// ## Enums with Data (Tagged Unions)
/// Unlike C-style enums that are just numbers, Rust enums can hold different data
/// for each variant. This is called a "tagged union" or "sum type".
///
/// ```rust
/// // Each variant can have its own fields:
/// enum PendingChange {
///     CreateKey { root: String, path: String },  // Has 2 fields
///     DeleteKey { root: String, path: String },  // Also 2 fields
///     SetValue { ... },                          // Has 5 fields!
/// }
/// ```
///
/// ## Derive Macros
/// The `#[derive(...)]` attribute auto-generates trait implementations:
/// - `Debug`: Allows printing with `{:?}` for debugging
/// - `Clone`: Allows creating copies with `.clone()`
/// - `Serialize/Deserialize`: Allows converting to/from JSON (via serde)
/// - `PartialEq`: Allows comparing with `==`
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PendingChange {
    /// Create a new registry key (folder)
    CreateKey {
        root: String,   // e.g., "HKEY_CURRENT_USER"
        path: String,   // e.g., "Software\\MyApp"
    },
    /// Delete an existing registry key and all its contents
    DeleteKey {
        root: String,
        path: String,
    },
    /// Set (create or update) a registry value
    SetValue {
        root: String,
        path: String,
        name: String,           // Value name (empty string = default value)
        value_type: String,     // e.g., "REG_SZ", "REG_DWORD"
        value_data: Vec<u8>,    // Raw bytes of the value
    },
    /// Delete a registry value
    DeleteValue {
        root: String,
        path: String,
        name: String,
    },
    /// Rename a registry value (delete old, create new with same data)
    RenameValue {
        root: String,
        path: String,
        old_name: String,
        new_name: String,
    },
}

/// Implementation block for PendingChange.
///
/// # Rust Concept: impl Blocks
/// Methods are defined in `impl` blocks, not inside the struct/enum definition.
/// This separates data layout from behavior, and allows multiple impl blocks.
impl PendingChange {
    /// Returns a human-readable description of this change.
    ///
    /// # The `match` Expression
    /// Rust's `match` is like a switch statement, but:
    /// - It MUST handle all possible variants (exhaustive)
    /// - It can destructure data from enum variants
    /// - It's an expression (returns a value)
    pub fn description(&self) -> String {
        match self {
            // Destructure the variant to access its fields
            PendingChange::CreateKey { path, .. } => format!("Create key: {}", path),
            PendingChange::DeleteKey { path, .. } => format!("Delete key: {}", path),
            PendingChange::SetValue { path, name, .. } => {
                format!("Set value: {}\\{}", path, name)
            }
            PendingChange::DeleteValue { path, name, .. } => {
                format!("Delete value: {}\\{}", path, name)
            }
            PendingChange::RenameValue {
                path,
                old_name,
                new_name,
                ..  // `..` means "ignore remaining fields"
            } => format!("Rename: {}\\{} -> {}", path, old_name, new_name),
        }
    }

    /// Returns the full registry path affected by this change.
    ///
    /// # Pattern Matching with `|` (Or Patterns)
    /// Multiple patterns can be combined with `|` when they have the same fields:
    /// ```rust
    /// match value {
    ///     PatternA { x, y } | PatternB { x, y } => { /* use x and y */ }
    /// }
    /// ```
    pub fn full_path(&self) -> String {
        match self {
            // These variants all have `root` and `path` fields, so combine them:
            PendingChange::CreateKey { root, path }
            | PendingChange::DeleteKey { root, path }
            | PendingChange::SetValue { root, path, .. }
            | PendingChange::DeleteValue { root, path, .. }
            | PendingChange::RenameValue { root, path, .. } => {
                if path.is_empty() {
                    root.clone()  // Clone because we need to return an owned String
                } else {
                    format!("{}\\{}", root, path)
                }
            }
        }
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// CONFLICT TYPES - When local and registry disagree
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Represents a conflict detected when trying to sync changes to the registry.
///
/// A conflict occurs when:
/// - We try to push a change, but the registry has been modified since we cached it
/// - We try to create something that already exists
/// - We try to modify something that was deleted
///
/// # Fields
/// - `change`: The pending change that caused the conflict
/// - `conflict_type`: What kind of conflict occurred
/// - `cached_lwt`: Last Write Time we have cached (when we last synced)
/// - `live_lwt`: Current Last Write Time in the registry
#[derive(Debug, Clone, PartialEq)]
pub struct SyncConflict {
    pub change: PendingChange,
    pub conflict_type: ConflictType,
    pub cached_lwt: u64,    // LWT = Last Write Time (Windows file time format)
    pub live_lwt: u64,
}

/// The type of conflict encountered during sync.
#[derive(Debug, Clone, PartialEq)]
pub enum ConflictType {
    /// The registry key was modified since we last synced
    KeyModified,
    /// The registry key was deleted
    KeyDeleted,
    /// The registry key already exists (for create)
    KeyAlreadyExists,
    /// A value was modified
    ValueModified,
}

/// How the user wants to resolve a conflict.
///
/// When a conflict is detected, the user must choose:
#[derive(Debug, Clone, PartialEq)]
pub enum ConflictResolution {
    /// Keep the local (SQLite) version, overwrite registry
    KeepLocal,
    /// Keep the registry version, discard local change
    KeepRegistry,
    /// Skip this change for now (leave it pending)
    Skip,
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// SYNC STATISTICS - Tracking sync state
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Statistics about the current sync state.
///
/// # Rust Concept: Default Trait
/// The `#[derive(Default)]` generates a `default()` method that creates
/// an instance with all fields set to their default values:
/// - Numbers: 0
/// - Option: None
/// - String: empty
/// - Vec: empty
#[derive(Debug, Clone, Default)]
pub struct SyncStats {
    pub pending_changes: usize,                     // Count of unsynced changes
    pub last_sync_to_registry: Option<Instant>,     // When we last pushed
    pub last_sync_from_registry: Option<Instant>,   // When we last pulled
    pub conflicts_detected: usize,                  // Conflicts found in last sync
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// MAIN SYNC STORE - The heart of the SQLite-first architecture
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// The main data store that wraps SQLite as the primary data source.
///
/// # Architecture
///
/// SyncStore is the bridge between the UI and both SQLite and the Windows Registry.
/// It implements a "working copy" pattern similar to version control systems:
///
/// ```text
/// User edits → SQLite (working copy) → Registry (on push)
/// Registry changes → SQLite (on pull) → UI sees updated data
/// ```
///
/// # Thread Safety with Arc
///
/// This struct uses `Arc` (Atomic Reference Counted) wrappers extensively.
/// This allows the store to be safely shared between:
/// - The main UI thread
/// - Background sync threads
/// - Event handlers
///
/// ## Why Arc?
/// ```rust
/// // Without Arc, this would be a compile error:
/// let store = SyncStore::new();
/// let store_clone = store.clone();  // Only works because fields are Arc
/// std::thread::spawn(move || {
///     store_clone.do_something();   // Can use in another thread!
/// });
/// store.do_something_else();        // Original still usable
/// ```
///
/// ## Why Mutex inside Arc?
/// `Arc` alone only allows read-only sharing. For mutable data, we need `Mutex`:
/// ```rust
/// let data: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
/// let data_clone = data.clone();
/// // To modify:
/// let mut guard = data.lock().unwrap();  // Acquire lock
/// guard.push("hello".to_string());       // Modify
/// // Lock released when `guard` goes out of scope
/// ```
///
/// ## Why AtomicBool/AtomicU64?
/// For simple values that are only read/written (not complex operations),
/// atomics are faster than Mutex because they don't require locking:
/// ```rust
/// let flag: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
/// flag.store(true, Ordering::SeqCst);           // Write
/// let value = flag.load(Ordering::Relaxed);     // Read
/// ```
#[derive(Clone)]  // Cloning an Arc just increments the reference count (cheap!)
pub struct SyncStore {
    /// Path to the SQLite database file (e.g., %APPDATA%/registry-editor/registry.db)
    db_path: Arc<PathBuf>,

    // ── Sync State (shared between threads) ──
    
    /// True while a sync operation is in progress
    pub is_syncing: Arc<AtomicBool>,
    /// Number of in-flight background key/value fetches. UI should repaint while nonzero.
    pub pending_fetches: Arc<AtomicU32>,
    /// Keys currently being fetched ("root:path"), prevents duplicate fetch threads.
    in_flight_fetches: Arc<Mutex<HashSet<String>>>,
    /// In-memory subkey cache: "root:path" -> sorted subkey names.
    /// Stored behind Arc so the main-thread clone is O(1) (ref-count bump only).
    subkey_cache: Arc<Mutex<HashMap<String, Arc<Vec<String>>>>>,
    /// In-memory value cache: "root:path" -> values.
    value_cache: Arc<Mutex<HashMap<String, Vec<RegistryValue>>>>,
    /// In-memory bookmark list, kept in sync with SQLite on every mutation.
    bookmarks_cache: Arc<Mutex<Vec<Bookmark>>>,
    /// In-memory pending changes list, kept in sync with SQLite on every mutation.
    pending_changes_cache: Arc<Mutex<Vec<(i64, PendingChange)>>>,
    /// Current progress (keys processed) during sync
    pub sync_progress: Arc<AtomicU64>,
    /// Total items to process during sync
    pub sync_total: Arc<AtomicU64>,
    /// Human-readable description of current sync item (for UI display)
    pub current_sync_item: Arc<Mutex<String>>,

    // ── Statistics ──
    
    /// Various statistics about sync state
    pub stats: Arc<Mutex<SyncStats>>,

    // ── Conflict Handling ──
    
    /// Conflicts detected during last sync attempt, waiting for user resolution
    pub pending_conflicts: Arc<Mutex<Vec<SyncConflict>>>,

    // ── Background Sync Settings ──
    
    /// Whether automatic background pulling is enabled
    pub auto_pull_enabled: Arc<AtomicBool>,
    /// How often to auto-pull (in seconds)
    pub auto_pull_interval_secs: Arc<Mutex<u64>>,
    /// Which root keys to include in auto-pull
    pub pull_roots: Arc<Mutex<Vec<RootKey>>>,
    /// Maximum depth to traverse during pull (None = unlimited)
    pub pull_max_depth: Arc<Mutex<Option<usize>>>,

    // ── Debug Logging ──
    
    /// Whether debug logging is enabled
    pub debug_enabled: Arc<AtomicBool>,
    /// Ring buffer of recent debug events (newest at end)
    pub debug_log: Arc<Mutex<Vec<DebugEvent>>>,
}

/// A debug event logged by the sync store.
#[derive(Debug, Clone)]
pub struct DebugEvent {
    /// When the event occurred (wall-clock time)
    pub timestamp: SystemTime,
    /// Category of the event
    pub category: DebugCategory,
    /// Human-readable description
    pub message: String,
}

/// Category of debug events for filtering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DebugCategory {
    /// Reading from Windows Registry
    RegistryRead,
    /// Writing to Windows Registry
    RegistryWrite,
    /// Reading from SQLite
    SqliteRead,
    /// Writing to SQLite
    SqliteWrite,
    /// Cache operations (memory)
    Cache,
}

/// Implementation of SyncStore - all the methods that make it work.
///
/// # Rust Concept: impl Blocks
///
/// Rust separates struct definition from method implementation.
/// You can have multiple `impl` blocks for the same type, which is useful for:
/// - Organizing code by functionality
/// - Conditional compilation
/// - Implementing traits
impl SyncStore {
    /// Creates a new SyncStore, initializing the SQLite database.
    ///
    /// # What Happens Here:
    /// 1. Find the config directory (%APPDATA% on Windows)
    /// 2. Create our app's folder if needed
    /// 3. Initialize SQLite database with our schema
    /// 4. Load initial statistics
    ///
    /// # Rust Concepts:
    /// - `unwrap_or_else`: Provides a fallback value if Result/Option is Err/None
    /// - `Arc::new()`: Wraps a value in an atomic reference counter
    /// - `AtomicBool::new(false)`: Creates an atomic boolean initialized to false
    pub fn new() -> Self {
        // Get the user's config directory (e.g., C:\Users\Name\AppData\Roaming)
        let mut db_dir = dirs::config_dir().unwrap_or_else(|| PathBuf::from("."));
        db_dir.push("registry-editor");
        std::fs::create_dir_all(&db_dir).ok();
        let db_path = db_dir.join("registry.db");

        let store = Self {
            db_path: Arc::new(db_path),
            is_syncing: Arc::new(AtomicBool::new(false)),
            pending_fetches: Arc::new(AtomicU32::new(0)),
            in_flight_fetches: Arc::new(Mutex::new(HashSet::new())),
            subkey_cache: Arc::new(Mutex::new(HashMap::<String, Arc<Vec<String>>>::new())),
            value_cache: Arc::new(Mutex::new(HashMap::new())),
            bookmarks_cache: Arc::new(Mutex::new(Vec::new())),
            pending_changes_cache: Arc::new(Mutex::new(Vec::new())),
            sync_progress: Arc::new(AtomicU64::new(0)),
            sync_total: Arc::new(AtomicU64::new(0)),
            current_sync_item: Arc::new(Mutex::new(String::new())),
            stats: Arc::new(Mutex::new(SyncStats::default())),
            pending_conflicts: Arc::new(Mutex::new(Vec::new())),
            auto_pull_enabled: Arc::new(AtomicBool::new(true)),
            auto_pull_interval_secs: Arc::new(Mutex::new(300)),
            pull_roots: Arc::new(Mutex::new(vec![
                RootKey::HkeyCurrentUser,
                RootKey::HkeyLocalMachine,
            ])),
            pull_max_depth: Arc::new(Mutex::new(Some(8))),
            debug_enabled: Arc::new(AtomicBool::new(false)),
            debug_log: Arc::new(Mutex::new(Vec::with_capacity(1000))),
        };

        // If database opens successfully, initialize schema and load stats
        // `if let Ok(x) = result` is a pattern that only runs if result is Ok
        if let Ok(conn) = store.open_db() {
            init_sync_schema(&conn);
            store.load_settings(&conn);
            store.refresh_stats(&conn);
            store.reload_pending_changes_cache(&conn);
            store.reload_bookmarks_cache(&conn);
        }

        // Run a WAL checkpoint in the background so a large WAL file from a
        // previous interrupted session doesn't slow down future open_db() calls.
        // Done asynchronously so it never blocks the UI or startup.
        let store_for_ckpt = store.clone();
        std::thread::spawn(move || {
            if let Ok(conn) = store_for_ckpt.open_db() {
                // PASSIVE: checkpoints whatever is possible without blocking writers.
                conn.execute_batch("PRAGMA wal_checkpoint(PASSIVE)").ok();
            }
        });

        store
    }

    /// Opens a connection to the SQLite database with optimized settings.
    ///
    /// # SQLite PRAGMAs Explained:
    /// - `journal_mode = WAL`: Write-Ahead Logging - faster writes, allows concurrent reads
    /// - `synchronous = NORMAL`: Balance between safety and speed
    /// - `cache_size = -8000`: Use 8MB of memory for caching (negative = KB)
    /// - `temp_store = MEMORY`: Keep temporary tables in RAM
    ///
    /// # The `?` Operator
    /// The `?` at the end of lines is the "try" operator. It:
    /// 1. If Ok(value), unwraps and continues
    /// 2. If Err(e), returns early with that error
    ///
    /// It's shorthand for:
    /// ```rust
    /// let conn = match Connection::open(path) {
    ///     Ok(c) => c,
    ///     Err(e) => return Err(e),
    /// };
    /// ```
    ///
    /// # Visibility: `pub(crate)`
    /// `pub(crate)` means "public within this crate, but not to external users"
    /// - `pub`: Anyone can use it
    /// - `pub(crate)`: Only code in this project
    /// - `pub(super)`: Only parent module
    /// - (no modifier): Private to this module only
    pub(crate) fn open_db(&self) -> Result<Connection, rusqlite::Error> {
        let conn = Connection::open(self.db_path.as_ref())?;
        // Set busy timeout to wait up to 5 seconds if database is locked
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA cache_size = -32000;
             PRAGMA temp_store = MEMORY;
             PRAGMA wal_autocheckpoint = 100;
             PRAGMA mmap_size = 268435456;
             PRAGMA page_size = 4096;",
        )?;
        Ok(conn)
    }

    /// Reloads statistics from the database.
    ///
    /// # Mutex Usage
    /// `self.stats.lock().unwrap()` does three things:
    /// 1. `lock()`: Waits to acquire exclusive access to the data
    /// 2. Returns a `Result` (could fail if another thread panicked while holding lock)
    /// 3. `unwrap()`: Panics if the lock is "poisoned" (usually fine in practice)
    ///
    /// The returned `MutexGuard` auto-unlocks when it goes out of scope.
    fn refresh_stats(&self, conn: &Connection) {
        let pending: usize = conn
            .query_row("SELECT COUNT(*) FROM pending_changes", [], |r| r.get(0))
            .unwrap_or(0);
        let mut stats = self.stats.lock().unwrap();
        stats.pending_changes = pending;
    }

    /// Optimize the database by running ANALYZE and VACUUM.
    /// 
    /// Call this periodically (e.g., on app startup or after large imports)
    /// to help SQLite's query planner make better decisions.
    /// 
    /// - `ANALYZE`: Updates table statistics for query optimization
    /// - `VACUUM`: Rebuilds the database file, reclaiming space and defragmenting
    pub fn optimize(&self) {
        if let Ok(conn) = self.open_db() {
            // Update query planner statistics
            conn.execute_batch("ANALYZE").ok();
        }
    }

    /// Compact the database by running VACUUM.
    /// 
    /// This can be slow for large databases but reclaims disk space
    /// and improves performance by defragmenting the file.
    pub fn vacuum(&self) {
        if let Ok(conn) = self.open_db() {
            conn.execute_batch("VACUUM").ok();
        }
    }

    fn reload_pending_changes_cache(&self, conn: &Connection) {
        let mut stmt = match conn
            .prepare("SELECT id, change_json FROM pending_changes ORDER BY created_at")
        {
            Ok(s) => s,
            Err(_) => return,
        };
        let changes: Vec<(i64, PendingChange)> = stmt
            .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)))
            .unwrap()
            .filter_map(|r| r.ok())
            .filter_map(|(id, json)| {
                serde_json::from_str::<PendingChange>(&json).ok().map(|c| (id, c))
            })
            .collect();
        *self.pending_changes_cache.lock().unwrap() = changes;
    }

    fn reload_bookmarks_cache(&self, conn: &Connection) {
        let mut stmt = match conn.prepare(
            "SELECT name, path, notes, color FROM bookmarks ORDER BY sort_order",
        ) {
            Ok(s) => s,
            Err(_) => return,
        };
        let bms: Vec<Bookmark> = stmt
            .query_map([], |row| {
                Ok(Bookmark {
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
            .collect();
        *self.bookmarks_cache.lock().unwrap() = bms;
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // Settings Persistence
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /// Load settings from the database.
    fn load_settings(&self, conn: &Connection) {
        // Load auto_pull_enabled
        if let Ok(val) = conn.query_row(
            "SELECT value FROM settings WHERE key = 'auto_pull_enabled'",
            [],
            |row| row.get::<_, String>(0),
        ) {
            self.auto_pull_enabled.store(val == "true", Ordering::SeqCst);
        }

        // Load auto_pull_interval_secs
        if let Ok(val) = conn.query_row(
            "SELECT value FROM settings WHERE key = 'auto_pull_interval_secs'",
            [],
            |row| row.get::<_, String>(0),
        ) {
            if let Ok(secs) = val.parse::<u64>() {
                *self.auto_pull_interval_secs.lock().unwrap() = secs;
            }
        }

        // Load pull_max_depth
        if let Ok(val) = conn.query_row(
            "SELECT value FROM settings WHERE key = 'pull_max_depth'",
            [],
            |row| row.get::<_, String>(0),
        ) {
            let depth = if val == "unlimited" {
                None
            } else {
                val.parse::<usize>().ok()
            };
            *self.pull_max_depth.lock().unwrap() = depth;
        }
    }

    /// Save a single setting to the database.
    pub fn save_setting(&self, key: &str, value: &str) {
        if let Ok(conn) = self.open_db() {
            conn.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES (?1, ?2)",
                params![key, value],
            ).ok();
        }
    }

    /// Load a single setting from the database.
    pub fn load_setting(&self, key: &str) -> Option<String> {
        let conn = self.open_db().ok()?;
        conn.query_row(
            "SELECT value FROM settings WHERE key = ?1",
            params![key],
            |row| row.get(0),
        ).ok()
    }

    /// Save auto_pull_enabled setting.
    pub fn save_auto_pull_enabled(&self) {
        let val = if self.auto_pull_enabled.load(Ordering::Relaxed) { "true" } else { "false" };
        self.save_setting("auto_pull_enabled", val);
    }

    /// Save auto_pull_interval_secs setting.
    pub fn save_auto_pull_interval(&self) {
        let secs = self.auto_pull_interval_secs.lock().unwrap().to_string();
        self.save_setting("auto_pull_interval_secs", &secs);
    }

    /// Save pull_max_depth setting.
    pub fn save_pull_max_depth(&self) {
        let val = match *self.pull_max_depth.lock().unwrap() {
            Some(d) => d.to_string(),
            None => "unlimited".to_string(),
        };
        self.save_setting("pull_max_depth", &val);
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // Debug Logging
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /// Log a debug event if debug mode is enabled.
    pub fn log_debug(&self, category: DebugCategory, message: impl Into<String>) {
        if !self.debug_enabled.load(Ordering::Relaxed) {
            return;
        }
        let event = DebugEvent {
            timestamp: SystemTime::now(),
            category,
            message: message.into(),
        };
        let mut log = self.debug_log.lock().unwrap();
        // Keep only the last 1000 events (ring buffer behavior)
        if log.len() >= 1000 {
            log.remove(0);
        }
        log.push(event);
    }

    /// Get a snapshot of the debug log for display.
    pub fn get_debug_log(&self) -> Vec<DebugEvent> {
        self.debug_log.lock().unwrap().clone()
    }

    /// Clear the debug log.
    pub fn clear_debug_log(&self) {
        self.debug_log.lock().unwrap().clear();
    }

    /// Returns true if there are any pending changes waiting to be synced.
    pub fn has_pending_changes(&self) -> bool {
        self.stats.lock().unwrap().pending_changes > 0
    }

    /// Returns the count of pending changes (for UI display).
    pub fn pending_change_count(&self) -> usize {
        self.stats.lock().unwrap().pending_changes
    }

    /// Returns the size of the SQLite database file in bytes.
    pub fn get_db_size_bytes(&self) -> u64 {
        std::fs::metadata(self.db_path.as_ref())
            .map(|m| m.len())
            .unwrap_or(0)
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // SQLite-First Read Operations
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    //
    // These methods read data from SQLite first, falling back to the live
    // registry only when the data hasn't been cached yet (lazy loading).

    /// Gets the child keys (subkeys) of a registry path.
    ///
    /// # Lazy Loading Pattern
    /// 1. First, try to get data from SQLite cache
    /// 2. If cache is empty, fetch from live registry
    /// 3. Store the fetched data in SQLite for next time
    ///
    /// This means the first access is slower (hits registry), but subsequent
    /// accesses are fast (SQLite only).
    ///
    /// # Parameters
    /// - `root`: The registry hive (HKEY_CURRENT_USER, etc.)
    /// - `path`: The path within that hive (e.g., "Software\\Microsoft")
    ///
    /// # Returns
    /// A list of subkey names (just the names, not full paths)
    pub fn get_subkeys(&self, root: &RootKey, path: &str) -> Vec<String> {
        // Try to open database connection
        // `match` is used here instead of `?` because we want to return Vec::new() on error
        let conn = match self.open_db() {
            Ok(c) => c,
            Err(_) => return Vec::new(),
        };

        let root_str = root.to_string();
        
        // Prepare SQL statement (SQL injection safe via parameterized queries)
        // ?1, ?2 are parameter placeholders that will be filled with actual values
        let mut stmt = match conn.prepare(
            "SELECT path FROM keys WHERE root = ?1 AND parent_path = ?2 AND NOT deleted ORDER BY name_lower",
        ) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };

        // Execute query and collect results
        // This uses Rust's iterator pattern: query_map -> filter_map -> map -> collect
        let results: Vec<String> = match stmt.query_map(params![root_str, path], |row| row.get::<_, String>(0)) {
            Ok(rows) => rows.filter_map(|r| r.ok())  // Skip any rows that failed to read
                .map(|full_path| {
                    // Extract just the key name from the full path
                    // e.g., "Software\\Microsoft\\Windows" -> "Windows"
                    full_path
                        .rsplit('\\')      // Split from right, get iterator
                        .next()            // Take first element (rightmost segment)
                        .unwrap_or(&full_path)  // Fallback to full path if no backslash
                        .to_string()
                })
                .collect(),  // Collect iterator into Vec<String>
            Err(_) => Vec::new(),
        };

        // If we have no cached data, do a one-time pull from registry
        // This is the "lazy loading" part
        if results.is_empty() {
            // Try to enumerate subkeys from the live Windows Registry
            if let Ok(subkeys) = registry::enumerate_subkeys(root, path) {
                // Cache each subkey in SQLite for future use
                for subkey in &subkeys {
                    let child_path = if path.is_empty() {
                        subkey.clone()
                    } else {
                        format!("{}\\{}", path, subkey)
                    };
                    // Get the Last Write Time from registry (for conflict detection later)
                    let lwt = registry::get_last_write_time(root, &child_path).unwrap_or(0);
                    // Cache the key (dirty=false because it matches registry)
                    self.cache_key(&conn, root, &child_path, lwt, false);
                }
                return subkeys;
            }
        }

        results
    }

    /// Gets subkeys from the in-memory cache — no disk I/O, safe to call every frame.
    /// Returns empty if not yet fetched. Use `fetch_subkeys_async` to populate.
    pub fn get_subkeys_cached_only(&self, root: &RootKey, path: &str) -> Arc<Vec<String>> {
        let key = format!("{}:{}", root, path);
        self.subkey_cache.lock().unwrap()
            .get(&key)
            .cloned()
            .unwrap_or_else(|| Arc::new(Vec::new()))
    }

    /// Returns true if subkeys for this path are already in the in-memory cache.
    pub fn has_cached_subkeys(&self, root: &RootKey, path: &str) -> bool {
        let key = format!("{}:{}", root, path);
        self.subkey_cache.lock().unwrap().contains_key(&key)
    }

    /// Returns `(subkeys, is_fetched)` in a single mutex acquire — safe to call every frame.
    /// The Arc clone is O(1); the main thread never copies the full Vec.
    pub fn get_subkeys_cached(&self, root: &RootKey, path: &str) -> (Arc<Vec<String>>, bool) {
        let key = format!("{}:{}", root, path);
        let cache = self.subkey_cache.lock().unwrap();
        match cache.get(&key) {
            Some(v) => (Arc::clone(v), true),
            None => (Arc::new(Vec::new()), false),
        }
    }

    /// Returns the number of paths currently in the subkey cache (for debug overlay).
    pub fn subkey_cache_len(&self) -> usize {
        self.subkey_cache.lock().unwrap().len()
    }

    /// Fetches subkeys from the registry on a background thread and caches them.
    /// No-ops if a fetch for this path is already in-flight.
    pub fn fetch_subkeys_async(&self, root: &RootKey, path: &str) {
        let key = format!("{}:{}", root, path);
        {
            let mut in_flight = self.in_flight_fetches.lock().unwrap();
            if !in_flight.insert(key.clone()) {
                return;
            }
        }
        self.pending_fetches.fetch_add(1, Ordering::Relaxed);
        let store = self.clone();
        let root = root.clone();
        let path = path.to_string();
        std::thread::spawn(move || {
            store.log_debug(DebugCategory::RegistryRead, format!("enumerate_subkeys: {}\\{}", root, path));
            if let Ok(subkeys) = registry::enumerate_subkeys(&root, &path) {
                // Wrap in Arc so both the cache and this thread can hold a reference.
                let cached = Arc::new(subkeys);
                
                // Pre-fetch one level deeper: check if each child has children.
                // This allows the UI to know immediately which nodes are leaves.
                // We collect all results first, then insert everything at once
                // to avoid the UI rendering partially-fetched state.
                let mut grandchildren_results: Vec<(String, Arc<Vec<String>>)> = Vec::new();
                
                for subkey in cached.iter() {
                    let child_path = if path.is_empty() {
                        subkey.clone()
                    } else {
                        format!("{}\\{}", path, subkey)
                    };
                    let child_key = format!("{}:{}", root, child_path);
                    
                    // Only fetch if not already cached
                    if !store.subkey_cache.lock().unwrap().contains_key(&child_key) {
                        let grandchildren = registry::enumerate_subkeys(&root, &child_path)
                            .unwrap_or_else(|_| Vec::new());
                        grandchildren_results.push((child_key, Arc::new(grandchildren)));
                    }
                }
                
                // Now insert everything at once: parent's subkeys + all grandchildren
                {
                    let mut cache = store.subkey_cache.lock().unwrap();
                    cache.insert(key.clone(), Arc::clone(&cached));
                    for (child_key, grandchildren) in grandchildren_results {
                        cache.insert(child_key, grandchildren);
                    }
                }
                
                // ── Signal UI immediately ─────────────────────────────────────────
                // The in-memory cache is now populated; decrement pending_fetches so
                // the repaint loop (pending_fetches > 0 → request_repaint) stops
                // waiting and shows the children on the next frame.
                // SQLite persistence happens AFTER this point and does NOT gate the UI.
                store.in_flight_fetches.lock().unwrap().remove(&key);
                store.pending_fetches.fetch_sub(1, Ordering::Relaxed);
                // ── Background SQLite persistence (does not affect UI) ────────────
                if let Ok(conn) = store.open_db() {
                    store.log_debug(DebugCategory::SqliteWrite, format!("cache {} subkeys for {}\\{}", cached.len(), root, path));
                    conn.execute_batch("BEGIN").ok();
                    for subkey in cached.iter() {
                        let child_path = if path.is_empty() {
                            subkey.clone()
                        } else {
                            format!("{}\\{}", path, subkey)
                        };
                        store.cache_key(&conn, &root, &child_path, 0, false);
                    }
                    conn.execute_batch("COMMIT").ok();
                }
            } else {
                store.in_flight_fetches.lock().unwrap().remove(&key);
                store.pending_fetches.fetch_sub(1, Ordering::Relaxed);
            }
        });
    }

    /// Gets the values stored under a registry key.
    ///
    /// # Registry Values vs Keys
    /// - **Keys** are like folders - they can contain other keys and values
    /// - **Values** are like files - they have a name, type, and data
    ///
    /// # Return Type: `Result<Vec<RegistryValue>, String>`
    /// This returns either:
    /// - `Ok(vec![...])`: A vector of values on success
    /// - `Err("error message")`: An error message on failure
    ///
    /// # The `.map_err()` Pattern
    /// `.map_err(|e| e.to_string())?` transforms any error type into a String
    /// before propagating it with `?`. This is useful when you want consistent
    /// error types across different error sources.
    pub fn get_values(&self, root: &RootKey, path: &str) -> Result<Vec<RegistryValue>, String> {
        let conn = self.open_db().map_err(|e| e.to_string())?;
        let root_str = root.to_string();

        // Check if we have this key cached; if not, fetch from registry
        if !self.has_cached_key(root, path) {
            self.pull_key_values(&conn, root, path)?;
        }

        // Query SQLite for all values under this key
        let mut stmt = conn
            .prepare(
                "SELECT value_name, value_type, value_data FROM key_values 
                 WHERE root = ?1 AND key_path = ?2 AND NOT deleted 
                 ORDER BY LOWER(value_name)",
            )
            .map_err(|e| e.to_string())?;

        let values: Vec<RegistryValue> = stmt
            .query_map(params![root_str, path], |row| {
                let name: String = row.get(0)?;
                let type_str: String = row.get(1)?;
                let data: Vec<u8> = row.get(2)?;
                Ok(RegistryValue {
                    name: if name == "(Default)" {
                        String::new()
                    } else {
                        name
                    },
                    data: deserialize_reg_value(&type_str, &data),
                })
            })
            .map_err(|e| e.to_string())?
            .filter_map(|r| r.ok())
            .collect();

        Ok(values)
    }

    /// Gets values from the in-memory cache — no disk I/O, safe to call every frame.
    /// Returns empty if not yet fetched. Use `fetch_values_async` to populate.
    pub fn get_values_cached_only(&self, root: &RootKey, path: &str) -> Vec<RegistryValue> {
        let key = format!("values:{}:{}", root, path);
        self.value_cache.lock().unwrap().get(&key).cloned().unwrap_or_default()
    }

    /// Returns true if values for this key are already in the in-memory cache.
    pub fn has_cached_values(&self, root: &RootKey, path: &str) -> bool {
        let key = format!("values:{}:{}", root, path);
        self.value_cache.lock().unwrap().contains_key(&key)
    }

    /// Fetches values for a key from the registry on a background thread and caches them.
    /// No-ops if a fetch for this path is already in-flight.
    pub fn fetch_values_async(&self, root: &RootKey, path: &str) {
        let key = format!("values:{}:{}", root, path);
        {
            let mut in_flight = self.in_flight_fetches.lock().unwrap();
            if !in_flight.insert(key.clone()) {
                return;
            }
        }
        self.pending_fetches.fetch_add(1, Ordering::Relaxed);
        let store = self.clone();
        let root = root.clone();
        let path = path.to_string();
        std::thread::spawn(move || {
            store.log_debug(DebugCategory::RegistryRead, format!("enumerate_values: {}\\{}", root, path));
            if let Ok(values) = registry::enumerate_values(&root, &path) {
                // Move values into cache (no clone needed — pull_key_values re-reads
                // from the registry for SQLite, so we don't need values after this).
                store.log_debug(DebugCategory::Cache, format!("cache {} values for {}\\{}", values.len(), root, path));
                store.value_cache.lock().unwrap().insert(key.clone(), values);
                // Signal UI immediately — same pattern as fetch_subkeys_async.
                store.in_flight_fetches.lock().unwrap().remove(&key);
                store.pending_fetches.fetch_sub(1, Ordering::Relaxed);
                // Background SQLite persistence (does not affect UI).
                if let Ok(conn) = store.open_db() {
                    let _ = store.pull_key_values(&conn, &root, &path);
                }
            } else {
                store.in_flight_fetches.lock().unwrap().remove(&key);
                store.pending_fetches.fetch_sub(1, Ordering::Relaxed);
            }
        });
    }

    /// Checks if a key exists in our SQLite cache.
    ///
    /// This is a simple existence check - returns true if the key row exists,
    /// regardless of whether it's marked as deleted.
    fn has_cached_key(&self, root: &RootKey, path: &str) -> bool {
        let conn = match self.open_db() {
            Ok(c) => c,
            Err(_) => return false,
        };
        let root_str = root.to_string();
        // query_row returns Ok if a row was found, Err if not
        // We don't care about the actual data, just whether it exists
        conn.query_row(
            "SELECT 1 FROM keys WHERE root = ?1 AND path = ?2",
            params![root_str, path],
            |_| Ok(()),  // Closure that ignores the row data
        )
        .is_ok()  // Convert Result to bool
    }

    /// Stores a key in the SQLite cache.
    ///
    /// # Parameters
    /// - `conn`: Database connection (reused to avoid opening multiple connections)
    /// - `root`: Registry hive
    /// - `path`: Full path within the hive
    /// - `lwt`: Last Write Time from the registry (for conflict detection)
    /// - `dirty`: Whether this is a local change that needs to be synced
    fn cache_key(&self, conn: &Connection, root: &RootKey, path: &str, lwt: u64, dirty: bool) {
        let root_str = root.to_string();
        // Extract just the key name from the full path
        let key_name = path.rsplit('\\').next().unwrap_or(path);
        let parent = parent_path(path);
        
        // INSERT OR REPLACE: If row exists, replace it; if not, insert new
        conn.execute(
            "INSERT OR REPLACE INTO keys (root, path, name_lower, parent_path, registry_lwt, local_lwt, dirty, deleted) 
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0)",
            params![root_str, path, key_name.to_lowercase(), parent, lwt, now_timestamp(), dirty],
        )
        .ok();  // .ok() discards any error (we don't want to fail on cache issues)
    }

    /// Pulls all values for a key from the registry and caches them in SQLite.
    ///
    /// This is called during lazy loading when we access a key we haven't cached yet.
    fn pull_key_values(
        &self,
        conn: &Connection,
        root: &RootKey,
        path: &str,
    ) -> Result<(), String> {
        // Fetch all values from the live registry
        let values = registry::enumerate_values(root, path)?;
        let root_str = root.to_string();

        // Use a transaction for atomic updates (all or nothing)
        // BEGIN starts the transaction
        conn.execute_batch("BEGIN").ok();

        // Mark key as cached with its current Last Write Time
        let lwt = registry::get_last_write_time(root, path).unwrap_or(0);
        self.cache_key(conn, root, path, lwt, false);

        // Cache all values
        for val in &values {
            // Registry uses empty string for default value; we display as "(Default)"
            let display_name = if val.name.is_empty() {
                "(Default)".to_string()
            } else {
                val.name.clone()
            };
            // Serialize the value for SQLite storage
            let (type_str, data) = serialize_reg_value(&val.data);
            conn.execute(
                "INSERT OR REPLACE INTO key_values 
                 (root, key_path, value_name, value_type, value_data, dirty, deleted)
                 VALUES (?1, ?2, ?3, ?4, ?5, 0, 0)",
                params![root_str, path, display_name, type_str, data],
            )
            .ok();
        }

        // COMMIT ends the transaction, applying all changes atomically
        conn.execute_batch("COMMIT").ok();
        Ok(())
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // SQLite-First Write Operations
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    //
    // These methods write to SQLite first, NOT directly to the registry.
    // Changes are marked as "pending" and only applied when the user syncs.

    /// Creates a new registry key in SQLite (does NOT create in registry yet).
    ///
    /// # SQLite-First Pattern
    /// 1. Insert the new key into SQLite with `dirty = 1`
    /// 2. Record a `PendingChange::CreateKey` for later sync
    /// 3. The key appears in the UI immediately
    /// 4. When user clicks "Push", we create it in the actual registry
    ///
    /// # Why This Pattern?
    /// - User can preview changes before committing
    /// - Multiple changes can be batched
    /// - Changes can be discarded without affecting registry
    pub fn create_key(&self, root: &RootKey, parent_path: &str, name: &str) -> Result<(), String> {
        let conn = self.open_db().map_err(|e| e.to_string())?;
        let root_str = root.to_string();

        // Build the full path for the new key
        let child_path = if parent_path.is_empty() {
            name.to_string()
        } else {
            format!("{}\\{}", parent_path, name)
        };

        self.log_debug(DebugCategory::SqliteWrite, format!("INSERT key: {}\\{}", root_str, child_path));

        // Insert into SQLite with dirty=1 (needs sync)
        // registry_lwt=0 because it doesn't exist in registry yet
        conn.execute(
            "INSERT OR REPLACE INTO keys (root, path, name_lower, parent_path, registry_lwt, local_lwt, dirty, deleted)
             VALUES (?1, ?2, ?3, ?4, 0, ?5, 1, 0)",
            params![
                root_str,
                child_path,
                name.to_lowercase(),
                parent_path,
                now_timestamp()
            ],
        )
        .map_err(|e| e.to_string())?;

        // Record the pending change so we know what to do on sync
        self.record_pending_change(
            &conn,
            &PendingChange::CreateKey {
                root: root_str,
                path: child_path,
            },
        );

        // Update the pending changes count
        self.refresh_stats(&conn);
        Ok(())
    }

    /// Delete a key in SQLite (marks as deleted, doesn't touch registry).
    pub fn delete_key(&self, root: &RootKey, parent_path: &str, name: &str) -> Result<(), String> {
        let conn = self.open_db().map_err(|e| e.to_string())?;
        let root_str = root.to_string();

        let child_path = if parent_path.is_empty() {
            name.to_string()
        } else {
            format!("{}\\{}", parent_path, name)
        };

        // Mark as deleted
        conn.execute(
            "UPDATE keys SET deleted = 1, dirty = 1, local_lwt = ?1 WHERE root = ?2 AND path = ?3",
            params![now_timestamp(), root_str, child_path],
        )
        .map_err(|e| e.to_string())?;

        // Also mark all descendant keys
        let like_pattern = format!("{}\\%", child_path.replace('%', "\\%").replace('_', "\\_"));
        conn.execute(
            "UPDATE keys SET deleted = 1, dirty = 1 WHERE root = ?1 AND path LIKE ?2 ESCAPE '\\'",
            params![root_str, like_pattern],
        )
        .ok();

        // Record pending change
        self.record_pending_change(
            &conn,
            &PendingChange::DeleteKey {
                root: root_str,
                path: child_path,
            },
        );

        self.refresh_stats(&conn);
        Ok(())
    }

    /// Set a value in SQLite (marks as dirty).
    pub fn set_value(
        &self,
        root: &RootKey,
        path: &str,
        name: &str,
        value: &RegValue,
    ) -> Result<(), String> {
        let conn = self.open_db().map_err(|e| e.to_string())?;
        let root_str = root.to_string();
        let display_name = if name.is_empty() {
            "(Default)".to_string()
        } else {
            name.to_string()
        };

        let (type_str, data) = serialize_reg_value(value);

        self.log_debug(DebugCategory::SqliteWrite, format!("INSERT value: {}\\{}\\{}", root_str, path, display_name));

        conn.execute(
            "INSERT OR REPLACE INTO key_values 
             (root, key_path, value_name, value_type, value_data, dirty, deleted)
             VALUES (?1, ?2, ?3, ?4, ?5, 1, 0)",
            params![root_str, path, display_name, type_str, data],
        )
        .map_err(|e| e.to_string())?;

        // Mark the key as dirty too
        conn.execute(
            "UPDATE keys SET dirty = 1, local_lwt = ?1 WHERE root = ?2 AND path = ?3",
            params![now_timestamp(), root_str, path],
        )
        .ok();

        // Record pending change
        self.record_pending_change(
            &conn,
            &PendingChange::SetValue {
                root: root_str,
                path: path.to_string(),
                name: display_name,
                value_type: type_str,
                value_data: data,
            },
        );

        self.refresh_stats(&conn);
        Ok(())
    }

    /// Delete a value in SQLite (marks as deleted).
    pub fn delete_value(&self, root: &RootKey, path: &str, name: &str) -> Result<(), String> {
        let conn = self.open_db().map_err(|e| e.to_string())?;
        let root_str = root.to_string();
        let display_name = if name.is_empty() {
            "(Default)".to_string()
        } else {
            name.to_string()
        };

        conn.execute(
            "UPDATE key_values SET deleted = 1, dirty = 1 
             WHERE root = ?1 AND key_path = ?2 AND value_name = ?3",
            params![root_str, path, display_name],
        )
        .map_err(|e| e.to_string())?;

        // Record pending change
        self.record_pending_change(
            &conn,
            &PendingChange::DeleteValue {
                root: root_str,
                path: path.to_string(),
                name: display_name,
            },
        );

        self.refresh_stats(&conn);
        Ok(())
    }

    /// Rename a value in SQLite.
    pub fn rename_value(
        &self,
        root: &RootKey,
        path: &str,
        old_name: &str,
        new_name: &str,
    ) -> Result<(), String> {
        let conn = self.open_db().map_err(|e| e.to_string())?;
        let root_str = root.to_string();

        let old_display = if old_name.is_empty() {
            "(Default)".to_string()
        } else {
            old_name.to_string()
        };
        let new_display = if new_name.is_empty() {
            "(Default)".to_string()
        } else {
            new_name.to_string()
        };

        // Get the current value
        let (type_str, data): (String, Vec<u8>) = conn
            .query_row(
                "SELECT value_type, value_data FROM key_values 
                 WHERE root = ?1 AND key_path = ?2 AND value_name = ?3",
                params![root_str, path, old_display],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(|e| e.to_string())?;

        conn.execute_batch("BEGIN").ok();

        // Mark old as deleted
        conn.execute(
            "UPDATE key_values SET deleted = 1, dirty = 1 
             WHERE root = ?1 AND key_path = ?2 AND value_name = ?3",
            params![root_str, path, old_display],
        )
        .ok();

        // Insert new
        conn.execute(
            "INSERT OR REPLACE INTO key_values 
             (root, key_path, value_name, value_type, value_data, dirty, deleted)
             VALUES (?1, ?2, ?3, ?4, ?5, 1, 0)",
            params![root_str, path, new_display, type_str, data],
        )
        .ok();

        conn.execute_batch("COMMIT").ok();

        // Record pending change
        self.record_pending_change(
            &conn,
            &PendingChange::RenameValue {
                root: root_str,
                path: path.to_string(),
                old_name: old_display,
                new_name: new_display,
            },
        );

        self.refresh_stats(&conn);
        Ok(())
    }

    // ── Pending Changes Management ──────────────────────────────────────────

    fn record_pending_change(&self, conn: &Connection, change: &PendingChange) {
        let json = serde_json::to_string(change).unwrap_or_default();
        conn.execute(
            "INSERT INTO pending_changes (change_json, created_at) VALUES (?1, ?2)",
            params![json, now_timestamp()],
        )
        .ok();
        self.reload_pending_changes_cache(conn);
    }

    pub fn get_pending_changes(&self) -> Vec<(i64, PendingChange)> {
        self.pending_changes_cache.lock().unwrap().clone()
    }

    pub fn discard_pending_change(&self, change_id: i64) {
        let conn = match self.open_db() {
            Ok(c) => c,
            Err(_) => return,
        };
        conn.execute("DELETE FROM pending_changes WHERE id = ?1", params![change_id])
            .ok();
        self.refresh_stats(&conn);
        self.reload_pending_changes_cache(&conn);
    }

    pub fn discard_all_pending_changes(&self) {
        let conn = match self.open_db() {
            Ok(c) => c,
            Err(_) => return,
        };
        conn.execute_batch(
            "BEGIN;
             UPDATE keys SET dirty = 0;
             DELETE FROM keys WHERE registry_lwt = 0;
             UPDATE key_values SET dirty = 0;
             DELETE FROM key_values WHERE dirty = 1;
             DELETE FROM pending_changes;
             COMMIT;",
        )
        .ok();
        self.refresh_stats(&conn);
        self.reload_pending_changes_cache(&conn);
    }

    // ── Sync to Registry (Push) ─────────────────────────────────────────────

    /// Push all pending changes to the live registry.
    /// Returns list of conflicts that need resolution.
    pub fn push_to_registry(&self) -> Vec<SyncConflict> {
        self.is_syncing.store(true, Ordering::SeqCst);
        let mut conflicts = Vec::new();

        let conn = match self.open_db() {
            Ok(c) => c,
            Err(_) => {
                self.is_syncing.store(false, Ordering::SeqCst);
                return conflicts;
            }
        };

        let changes = self.get_pending_changes();
        self.sync_total.store(changes.len() as u64, Ordering::SeqCst);
        self.sync_progress.store(0, Ordering::SeqCst);

        for (id, change) in &changes {
            *self.current_sync_item.lock().unwrap() = change.description();
            self.sync_progress.fetch_add(1, Ordering::SeqCst);

            self.log_debug(DebugCategory::RegistryWrite, format!("push: {}", change.description()));

            match self.apply_change_to_registry(&conn, change.clone()) {
                Ok(()) => {
                    // Change applied successfully, remove from pending
                    conn.execute("DELETE FROM pending_changes WHERE id = ?1", params![id])
                        .ok();
                }
                Err(conflict) => {
                    conflicts.push(conflict);
                }
            }
        }

        // Update stats
        {
            let mut stats = self.stats.lock().unwrap();
            stats.last_sync_to_registry = Some(Instant::now());
        }
        self.refresh_stats(&conn);
        self.is_syncing.store(false, Ordering::SeqCst);

        conflicts
    }

    fn apply_change_to_registry(
        &self,
        conn: &Connection,
        change: PendingChange,
    ) -> Result<(), SyncConflict> {
        match &change {
            PendingChange::CreateKey { root, path } => {
                let root_key = RootKey::from_name(root).ok_or_else(|| SyncConflict {
                    change: change.clone(),
                    conflict_type: ConflictType::KeyDeleted,
                    cached_lwt: 0,
                    live_lwt: 0,
                })?;

                let parent = parent_path(path);
                let name = path.rsplit('\\').next().unwrap_or(path);

                // Check if key already exists
                if registry::key_exists(&root_key, path) {
                    return Err(SyncConflict {
                        change: change.clone(),
                        conflict_type: ConflictType::KeyAlreadyExists,
                        cached_lwt: 0,
                        live_lwt: registry::get_last_write_time(&root_key, path).unwrap_or(0),
                    });
                }

                registry::create_key(&root_key, &parent, name).map_err(|_| SyncConflict {
                    change: change.clone(),
                    conflict_type: ConflictType::KeyDeleted,
                    cached_lwt: 0,
                    live_lwt: 0,
                })?;

                // Update local cache with new lwt
                let new_lwt = registry::get_last_write_time(&root_key, path).unwrap_or(0);
                conn.execute(
                    "UPDATE keys SET registry_lwt = ?1, dirty = 0 WHERE root = ?2 AND path = ?3",
                    params![new_lwt, root, path],
                )
                .ok();

                Ok(())
            }

            PendingChange::DeleteKey { root, path } => {
                let root_key = RootKey::from_name(root).ok_or_else(|| SyncConflict {
                    change: change.clone(),
                    conflict_type: ConflictType::KeyDeleted,
                    cached_lwt: 0,
                    live_lwt: 0,
                })?;

                let parent = parent_path(path);
                let name = path.rsplit('\\').next().unwrap_or(path);

                // Check for conflicts - was the key modified since we cached it?
                let cached_lwt: u64 = conn
                    .query_row(
                        "SELECT registry_lwt FROM keys WHERE root = ?1 AND path = ?2",
                        params![root, path],
                        |row| row.get(0),
                    )
                    .unwrap_or(0);

                let live_lwt = registry::get_last_write_time(&root_key, path).unwrap_or(0);

                if live_lwt != 0 && cached_lwt != 0 && live_lwt != cached_lwt {
                    return Err(SyncConflict {
                        change: change.clone(),
                        conflict_type: ConflictType::KeyModified,
                        cached_lwt,
                        live_lwt,
                    });
                }

                if registry::key_exists(&root_key, path) {
                    registry::delete_key(&root_key, &parent, name).map_err(|_| SyncConflict {
                        change: change.clone(),
                        conflict_type: ConflictType::KeyModified,
                        cached_lwt,
                        live_lwt,
                    })?;
                }

                // Remove from local cache
                conn.execute(
                    "DELETE FROM keys WHERE root = ?1 AND path = ?2",
                    params![root, path],
                )
                .ok();

                Ok(())
            }

            PendingChange::SetValue {
                root,
                path,
                name,
                value_type,
                value_data,
            } => {
                let root_key = RootKey::from_name(root).ok_or_else(|| SyncConflict {
                    change: change.clone(),
                    conflict_type: ConflictType::KeyDeleted,
                    cached_lwt: 0,
                    live_lwt: 0,
                })?;

                // Check for key modification conflict
                let cached_lwt: u64 = conn
                    .query_row(
                        "SELECT registry_lwt FROM keys WHERE root = ?1 AND path = ?2",
                        params![root, path],
                        |row| row.get(0),
                    )
                    .unwrap_or(0);

                let live_lwt = registry::get_last_write_time(&root_key, path).unwrap_or(0);

                if live_lwt != 0 && cached_lwt != 0 && live_lwt != cached_lwt {
                    return Err(SyncConflict {
                        change: change.clone(),
                        conflict_type: ConflictType::ValueModified,
                        cached_lwt,
                        live_lwt,
                    });
                }

                let val_name = if name == "(Default)" { "" } else { name.as_str() };
                let value = deserialize_reg_value(value_type, value_data);

                registry::set_value(&root_key, path, val_name, &value).map_err(|_| SyncConflict {
                    change: change.clone(),
                    conflict_type: ConflictType::KeyModified,
                    cached_lwt,
                    live_lwt,
                })?;

                // Update cached lwt
                let new_lwt = registry::get_last_write_time(&root_key, path).unwrap_or(0);
                conn.execute(
                    "UPDATE keys SET registry_lwt = ?1 WHERE root = ?2 AND path = ?3",
                    params![new_lwt, root, path],
                )
                .ok();
                conn.execute(
                    "UPDATE key_values SET dirty = 0 WHERE root = ?1 AND key_path = ?2 AND value_name = ?3",
                    params![root, path, name],
                )
                .ok();

                Ok(())
            }

            PendingChange::DeleteValue {
                root,
                path,
                name,
            } => {
                let root_key = RootKey::from_name(root).ok_or_else(|| SyncConflict {
                    change: change.clone(),
                    conflict_type: ConflictType::KeyDeleted,
                    cached_lwt: 0,
                    live_lwt: 0,
                })?;

                let val_name = if name == "(Default)" { "" } else { name.as_str() };

                // Try to delete (ignore errors if already gone)
                registry::delete_value(&root_key, path, val_name).ok();

                // Remove from local cache
                conn.execute(
                    "DELETE FROM key_values WHERE root = ?1 AND key_path = ?2 AND value_name = ?3",
                    params![root, path, name],
                )
                .ok();

                Ok(())
            }

            PendingChange::RenameValue {
                root,
                path,
                old_name,
                new_name,
            } => {
                let root_key = RootKey::from_name(root).ok_or_else(|| SyncConflict {
                    change: change.clone(),
                    conflict_type: ConflictType::KeyDeleted,
                    cached_lwt: 0,
                    live_lwt: 0,
                })?;

                let old_val = if old_name == "(Default)" {
                    ""
                } else {
                    old_name.as_str()
                };
                let new_val = if new_name == "(Default)" {
                    ""
                } else {
                    new_name.as_str()
                };

                registry::rename_value(&root_key, path, old_val, new_val).map_err(|_| {
                    SyncConflict {
                        change: change.clone(),
                        conflict_type: ConflictType::ValueModified,
                        cached_lwt: 0,
                        live_lwt: 0,
                    }
                })?;

                // Update local cache
                conn.execute(
                    "DELETE FROM key_values WHERE root = ?1 AND key_path = ?2 AND value_name = ?3",
                    params![root, path, old_name],
                )
                .ok();
                conn.execute(
                    "UPDATE key_values SET dirty = 0 WHERE root = ?1 AND key_path = ?2 AND value_name = ?3",
                    params![root, path, new_name],
                )
                .ok();

                Ok(())
            }
        }
    }

    /// Force-apply a specific change, ignoring conflicts.
    pub fn force_push_change(&self, change: &PendingChange) -> Result<(), String> {
        let conn = self.open_db().map_err(|e| e.to_string())?;

        // Find and remove the pending change entry
        let changes = self.get_pending_changes();
        for (id, c) in &changes {
            if c == change {
                // Apply regardless of conflict
                match change {
                    PendingChange::CreateKey { root, path } => {
                        let root_key =
                            RootKey::from_name(root).ok_or("Invalid root".to_string())?;
                        let parent = parent_path(path);
                        let name = path.rsplit('\\').next().unwrap_or(path);
                        registry::create_key(&root_key, &parent, name)?;
                    }
                    PendingChange::DeleteKey { root, path } => {
                        let root_key =
                            RootKey::from_name(root).ok_or("Invalid root".to_string())?;
                        let parent = parent_path(path);
                        let name = path.rsplit('\\').next().unwrap_or(path);
                        registry::delete_key(&root_key, &parent, name)?;
                    }
                    PendingChange::SetValue {
                        root,
                        path,
                        name,
                        value_type,
                        value_data,
                    } => {
                        let root_key =
                            RootKey::from_name(root).ok_or("Invalid root".to_string())?;
                        let val_name = if name == "(Default)" { "" } else { name.as_str() };
                        let value = deserialize_reg_value(value_type, value_data);
                        registry::set_value(&root_key, path, val_name, &value)?;
                    }
                    PendingChange::DeleteValue { root, path, name } => {
                        let root_key =
                            RootKey::from_name(root).ok_or("Invalid root".to_string())?;
                        let val_name = if name == "(Default)" { "" } else { name.as_str() };
                        registry::delete_value(&root_key, path, val_name)?;
                    }
                    PendingChange::RenameValue {
                        root,
                        path,
                        old_name,
                        new_name,
                    } => {
                        let root_key =
                            RootKey::from_name(root).ok_or("Invalid root".to_string())?;
                        let old_val = if old_name == "(Default)" {
                            ""
                        } else {
                            old_name.as_str()
                        };
                        let new_val = if new_name == "(Default)" {
                            ""
                        } else {
                            new_name.as_str()
                        };
                        registry::rename_value(&root_key, path, old_val, new_val)?;
                    }
                }

                conn.execute("DELETE FROM pending_changes WHERE id = ?1", params![id])
                    .ok();
                break;
            }
        }

        self.refresh_stats(&conn);
        Ok(())
    }

    // ── Sync from Registry (Pull) ───────────────────────────────────────────

    /// Pull latest registry state into SQLite.
    pub fn pull_from_registry(&self) {
        self.is_syncing.store(true, Ordering::SeqCst);
        self.sync_progress.store(0, Ordering::SeqCst);

        let conn = match self.open_db() {
            Ok(c) => c,
            Err(_) => {
                self.is_syncing.store(false, Ordering::SeqCst);
                return;
            }
        };

        let roots = self.pull_roots.lock().unwrap().clone();
        let max_depth = *self.pull_max_depth.lock().unwrap();

        let mut visited: HashSet<String> = HashSet::new();

        conn.execute("BEGIN", []).ok();

        for root in &roots {
            self.walk_and_pull(root, "", max_depth, &conn, &mut visited, 0);
        }

        conn.execute("COMMIT", []).ok();

        {
            let mut stats = self.stats.lock().unwrap();
            stats.last_sync_from_registry = Some(Instant::now());
        }

        self.is_syncing.store(false, Ordering::SeqCst);
    }

    fn walk_and_pull(
        &self,
        root: &RootKey,
        path: &str,
        max_depth: Option<usize>,
        conn: &Connection,
        visited: &mut HashSet<String>,
        depth: usize,
    ) {
        if let Some(md) = max_depth {
            if depth > md {
                return;
            }
        }

        let root_str = root.to_string();
        let full_key = format!("{}\\{}", root_str, path);
        visited.insert(full_key.clone());

        self.sync_progress.fetch_add(1, Ordering::SeqCst);
        if self.sync_progress.load(Ordering::Relaxed) % 200 == 0 {
            *self.current_sync_item.lock().unwrap() = full_key.clone();
        }

        if !path.is_empty() {
            let lwt = registry::get_last_write_time(root, path).unwrap_or(0);

            // Check if key is dirty locally - don't overwrite local changes
            let is_dirty: bool = conn
                .query_row(
                    "SELECT dirty FROM keys WHERE root = ?1 AND path = ?2",
                    params![root_str, path],
                    |row| row.get(0),
                )
                .unwrap_or(false);

            if !is_dirty {
                let key_name = path.rsplit('\\').next().unwrap_or(path);
                let parent = parent_path(path);
                conn.execute(
                    "INSERT OR REPLACE INTO keys (root, path, name_lower, parent_path, registry_lwt, local_lwt, dirty, deleted)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0, 0)",
                    params![root_str, path, key_name.to_lowercase(), parent, lwt, now_timestamp()],
                )
                .ok();

                // Pull values too
                if let Ok(vals) = registry::enumerate_values(root, path) {
                    // Only update non-dirty values
                    for val in vals {
                        let display_name = if val.name.is_empty() {
                            "(Default)".to_string()
                        } else {
                            val.name.clone()
                        };

                        let is_val_dirty: bool = conn
                            .query_row(
                                "SELECT dirty FROM key_values WHERE root = ?1 AND key_path = ?2 AND value_name = ?3",
                                params![root_str, path, display_name],
                                |row| row.get(0),
                            )
                            .unwrap_or(false);

                        if !is_val_dirty {
                            let (type_str, data) = serialize_reg_value(&val.data);
                            conn.execute(
                                "INSERT OR REPLACE INTO key_values 
                                 (root, key_path, value_name, value_type, value_data, dirty, deleted)
                                 VALUES (?1, ?2, ?3, ?4, ?5, 0, 0)",
                                params![root_str, path, display_name, type_str, data],
                            )
                            .ok();
                        }
                    }
                }
            }
        }

        // Recurse into subkeys
        if let Ok(subkeys) = registry::enumerate_subkeys(root, path) {
            for subkey in subkeys {
                let child_path = if path.is_empty() {
                    subkey
                } else {
                    format!("{}\\{}", path, subkey)
                };
                self.walk_and_pull(root, &child_path, max_depth, conn, visited, depth + 1);
            }
        }
    }

    /// Refresh a single key from the registry (if not dirty).
    pub fn refresh_key(&self, root: &RootKey, path: &str) {
        let conn = match self.open_db() {
            Ok(c) => c,
            Err(_) => return,
        };

        let root_str = root.to_string();

        // Check if dirty
        let is_dirty: bool = conn
            .query_row(
                "SELECT dirty FROM keys WHERE root = ?1 AND path = ?2",
                params![root_str, path],
                |row| row.get(0),
            )
            .unwrap_or(false);

        if !is_dirty {
            self.pull_key_values(&conn, root, path).ok();
        }
    }

    // ── Background Sync Loop ────────────────────────────────────────────────

    pub fn start_background_pull(&self) {
        let store = self.clone();
        std::thread::spawn(move || {
            // Initial pull
            store.pull_from_registry();

            loop {
                std::thread::sleep(Duration::from_secs(5));

                if !store.auto_pull_enabled.load(Ordering::Relaxed) {
                    continue;
                }

                let interval = *store.auto_pull_interval_secs.lock().unwrap();
                let should_pull = {
                    let stats = store.stats.lock().unwrap();
                    match stats.last_sync_from_registry {
                        Some(t) => t.elapsed().as_secs() >= interval,
                        None => true,
                    }
                };

                if should_pull && !store.is_syncing.load(Ordering::Relaxed) {
                    store.pull_from_registry();
                }
            }
        });
    }


    /// Pull from registry on a background thread (non-blocking).
    /// 
    /// Use `is_syncing` to check if still running.
    pub fn pull_from_registry_async(&self) {
        if self.is_syncing.load(Ordering::Relaxed) {
            return; // Already syncing
        }
        let store = self.clone();
        std::thread::spawn(move || {
            store.pull_from_registry();
        });
    }

    /// Push to registry on a background thread (non-blocking).
    /// 
    /// Returns immediately. Check `is_syncing` for completion.
    /// Conflicts will be stored in `pending_conflicts` field.
    pub fn push_to_registry_async(&self) {
        if self.is_syncing.load(Ordering::Relaxed) {
            return; // Already syncing
        }
        let store = self.clone();
        std::thread::spawn(move || {
            let conflicts = store.push_to_registry();
            if !conflicts.is_empty() {
                *store.pending_conflicts.lock().unwrap() = conflicts;
            }
        });
    }

    /// Get any conflicts from the last async push.
    pub fn take_pending_conflicts(&self) -> Vec<SyncConflict> {
        std::mem::take(&mut *self.pending_conflicts.lock().unwrap())
    }

    // ── Bookmarks ───────────────────────────────────────────────────────────

    pub fn get_bookmarks(&self) -> Vec<Bookmark> {
        self.bookmarks_cache.lock().unwrap().clone()
    }

    pub fn add_bookmark(&self, bm: &Bookmark) {
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
        self.reload_bookmarks_cache(&conn);
    }

    pub fn remove_bookmark(&self, path: &str) {
        let conn = match self.open_db() {
            Ok(c) => c,
            Err(_) => return,
        };
        conn.execute("DELETE FROM bookmarks WHERE path = ?1", params![path]).ok();
        self.reload_bookmarks_cache(&conn);
    }

    pub fn update_bookmark(&self, path: &str, bm: &Bookmark) {
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
        self.reload_bookmarks_cache(&conn);
    }

    pub fn is_bookmarked(&self, path: &str) -> bool {
        self.bookmarks_cache.lock().unwrap().iter().any(|b| b.path == path)
    }

    pub fn move_bookmark(&self, path: &str, direction: i32) {
        let conn = match self.open_db() {
            Ok(c) => c,
            Err(_) => return,
        };
        let current_order: i64 = match conn.query_row(
            "SELECT sort_order FROM bookmarks WHERE path = ?1",
            params![path],
            |r| r.get(0),
        ) {
            Ok(o) => o,
            Err(_) => return,
        };
        let swap_order = current_order + direction as i64;
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
        self.reload_bookmarks_cache(&conn);
    }

    /// Check if a key exists in SQLite cache
    pub fn key_exists(&self, root: &RootKey, path: &str) -> bool {
        if path.is_empty() {
            return true;
        }
        let conn = match self.open_db() {
            Ok(c) => c,
            Err(_) => return false,
        };
        let root_str = root.to_string();
        conn.query_row(
            "SELECT 1 FROM keys WHERE root = ?1 AND path = ?2 AND NOT deleted",
            params![root_str, path],
            |_| Ok(()),
        )
        .is_ok()
    }

    // ── Search (from SQLite) ────────────────────────────────────────────────

    /// Search keys using plain text (LIKE query)
    pub fn search_keys(&self, query: &str, case_sensitive: bool) -> Vec<(RootKey, String)> {
        let conn = match self.open_db() {
            Ok(c) => c,
            Err(_) => return Vec::new(),
        };

        let like = format!(
            "%{}%",
            query
                .to_lowercase()
                .replace('%', "\\%")
                .replace('_', "\\_")
        );

        let mut stmt = conn
            .prepare(
                "SELECT root, path FROM keys WHERE name_lower LIKE ?1 ESCAPE '\\' AND NOT deleted",
            )
            .unwrap();

        stmt.query_map(params![like], |row| {
            let root_str: String = row.get(0)?;
            let path: String = row.get(1)?;
            Ok((root_str, path))
        })
        .unwrap()
        .filter_map(|r| r.ok())
        .filter(|(_, path)| {
            if case_sensitive {
                let name = path.rsplit('\\').next().unwrap_or(path);
                name.contains(query)
            } else {
                true
            }
        })
        .filter_map(|(root_str, path)| {
            RootKey::from_name(&root_str).map(|r| (r, path))
        })
        .collect()
    }

    /// Search keys using regex pattern (fetches all keys, filters with regex)
    pub fn search_keys_regex(&self, pattern: &str, case_sensitive: bool) -> Vec<(RootKey, String)> {
        let conn = match self.open_db() {
            Ok(c) => c,
            Err(_) => return Vec::new(),
        };

        // Build regex with optional case-insensitivity
        let regex_pattern = if case_sensitive {
            pattern.to_string()
        } else {
            format!("(?i){}", pattern)
        };
        let regex = match regex::Regex::new(&regex_pattern) {
            Ok(r) => r,
            Err(_) => return Vec::new(),
        };

        // Fetch all non-deleted keys
        let mut stmt = match conn.prepare("SELECT root, path FROM keys WHERE NOT deleted") {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };

        stmt.query_map([], |row| {
            let root_str: String = row.get(0)?;
            let path: String = row.get(1)?;
            Ok((root_str, path))
        })
        .unwrap_or_else(|_| panic!("query failed"))
        .filter_map(|r| r.ok())
        .filter(|(_, path)| {
            // Match regex against the key name (last component of path)
            let name = path.rsplit('\\').next().unwrap_or(path);
            regex.is_match(name)
        })
        .filter_map(|(root_str, path)| {
            RootKey::from_name(&root_str).map(|r| (r, path))
        })
        .collect()
    }

    /// Search values from SQLite database.
    /// Returns: (root, path, value_name, value_type, value_data_text)
    pub fn search_values(
        &self,
        query: &str,
        case_sensitive: bool,
        search_names: bool,
        search_data: bool,
        value_type_filter: Option<&str>,
    ) -> Vec<CachedValueMatch> {
        let conn = match self.open_db() {
            Ok(c) => c,
            Err(_) => return Vec::new(),
        };

        let like = format!(
            "%{}%",
            query
                .to_lowercase()
                .replace('%', "\\%")
                .replace('_', "\\_")
        );

        // Build dynamic SQL based on search options
        let mut conditions = vec!["NOT deleted"];
        let mut params_vec: Vec<String> = Vec::new();

        if search_names && search_data {
            conditions.push("(LOWER(value_name) LIKE ?1 ESCAPE '\\' OR LOWER(CAST(value_data AS TEXT)) LIKE ?1 ESCAPE '\\')");
            params_vec.push(like.clone());
        } else if search_names {
            conditions.push("LOWER(value_name) LIKE ?1 ESCAPE '\\'");
            params_vec.push(like.clone());
        } else if search_data {
            conditions.push("LOWER(CAST(value_data AS TEXT)) LIKE ?1 ESCAPE '\\'");
            params_vec.push(like.clone());
        } else {
            return Vec::new();
        }

        if let Some(vt) = value_type_filter {
            conditions.push("value_type = ?2");
            params_vec.push(vt.to_string());
        }

        let sql = format!(
            "SELECT root, key_path, value_name, value_type, value_data FROM key_values WHERE {}",
            conditions.join(" AND ")
        );

        let mut stmt = match conn.prepare(&sql) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };

        // Execute query based on parameter count
        let rows: Vec<(String, String, String, String, Vec<u8>)> = if params_vec.len() == 1 {
            stmt.query_map(params![params_vec[0].clone()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Vec<u8>>(4)?,
                ))
            })
            .unwrap_or_else(|_| panic!("query failed"))
            .filter_map(|r| r.ok())
            .collect()
        } else {
            stmt.query_map(params![params_vec[0].clone(), params_vec[1].clone()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Vec<u8>>(4)?,
                ))
            })
            .unwrap_or_else(|_| panic!("query failed"))
            .filter_map(|r| r.ok())
            .collect()
        };

        let results: Vec<CachedValueMatch> = rows
            .into_iter()
            .filter_map(|(root_str, path, name, vtype, data)| {
                let root = RootKey::from_name(&root_str)?;
                let value = deserialize_reg_value(&vtype, &data);
                let data_text = value.display_data();

                // Apply case-sensitive filtering
                let name_lower = name.to_lowercase();
                let data_lower = data_text.to_lowercase();
                let query_lower = query.to_lowercase();

                let name_matches = search_names && name_lower.contains(&query_lower);
                let data_matches = search_data && data_lower.contains(&query_lower);

                if case_sensitive {
                    // Re-check with exact case
                    let exact_name_match = search_names && name.contains(query);
                    let exact_data_match = search_data && data_text.contains(query);
                    if !exact_name_match && !exact_data_match {
                        return None;
                    }
                } else if !name_matches && !data_matches {
                    return None;
                }

                Some(CachedValueMatch {
                    root,
                    path,
                    value_name: name,
                    value_type: vtype,
                    value_data_text: data_text,
                    matched_name: name_matches,
                    matched_data: data_matches,
                })
            })
            .collect();

        results
    }

    /// Search values using regex pattern (fetches all values, filters with regex)
    pub fn search_values_regex(
        &self,
        pattern: &str,
        case_sensitive: bool,
        search_names: bool,
        search_data: bool,
        value_type_filter: Option<&str>,
    ) -> Vec<CachedValueMatch> {
        let conn = match self.open_db() {
            Ok(c) => c,
            Err(_) => return Vec::new(),
        };

        // Build regex with optional case-insensitivity
        let regex_pattern = if case_sensitive {
            pattern.to_string()
        } else {
            format!("(?i){}", pattern)
        };
        let regex = match regex::Regex::new(&regex_pattern) {
            Ok(r) => r,
            Err(_) => return Vec::new(),
        };

        // Build SQL with optional value type filter
        let sql = if let Some(vt) = value_type_filter {
            format!(
                "SELECT root, key_path, value_name, value_type, value_data FROM key_values WHERE NOT deleted AND value_type = '{}'",
                vt.replace('\'', "''")
            )
        } else {
            "SELECT root, key_path, value_name, value_type, value_data FROM key_values WHERE NOT deleted".to_string()
        };

        let mut stmt = match conn.prepare(&sql) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };

        let rows: Vec<(String, String, String, String, Vec<u8>)> = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Vec<u8>>(4)?,
                ))
            })
            .unwrap_or_else(|_| panic!("query failed"))
            .filter_map(|r| r.ok())
            .collect();

        rows.into_iter()
            .filter_map(|(root_str, path, name, vtype, data)| {
                let root = RootKey::from_name(&root_str)?;
                let value = deserialize_reg_value(&vtype, &data);
                let data_text = value.display_data();

                // Match regex against name and/or data
                let name_matches = search_names && regex.is_match(&name);
                let data_matches = search_data && regex.is_match(&data_text);

                if !name_matches && !data_matches {
                    return None;
                }

                Some(CachedValueMatch {
                    root,
                    path,
                    value_name: name,
                    value_type: vtype,
                    value_data_text: data_text,
                    matched_name: name_matches,
                    matched_data: data_matches,
                })
            })
            .collect()
    }

    /// Comprehensive search across keys and values in the SQLite cache.
    /// Supports both plain text (LIKE) and regex searches.
    pub fn search(
        &self,
        query: &str,
        options: &CachedSearchOptions,
    ) -> Vec<CachedSearchResult> {
        let mut results = Vec::new();
        let max = options.max_results;

        // Search keys - use regex or plain text method
        if options.search_keys {
            let key_matches = if options.use_regex {
                self.search_keys_regex(query, options.case_sensitive)
            } else {
                self.search_keys(query, options.case_sensitive)
            };
            
            for (root, path) in key_matches {
                if results.len() >= max {
                    break;
                }
                if !options.roots.is_empty() && !options.roots.contains(&root) {
                    continue;
                }
                results.push(CachedSearchResult {
                    root,
                    path,
                    match_type: CachedMatchType::KeyName,
                    value_name: None,
                    value_data: None,
                    value_type: None,
                });
            }
        }

        // Search values - use regex or plain text method
        if (options.search_value_names || options.search_value_data) && results.len() < max {
            let value_matches = if options.use_regex {
                self.search_values_regex(
                    query,
                    options.case_sensitive,
                    options.search_value_names,
                    options.search_value_data,
                    options.value_type_filter.as_deref(),
                )
            } else {
                self.search_values(
                    query,
                    options.case_sensitive,
                    options.search_value_names,
                    options.search_value_data,
                    options.value_type_filter.as_deref(),
                )
            };

            for m in value_matches {
                if results.len() >= max {
                    break;
                }
                if !options.roots.is_empty() && !options.roots.contains(&m.root) {
                    continue;
                }
                results.push(CachedSearchResult {
                    root: m.root,
                    path: m.path,
                    match_type: if m.matched_name {
                        CachedMatchType::ValueName
                    } else {
                        CachedMatchType::ValueData
                    },
                    value_name: Some(m.value_name),
                    value_data: Some(m.value_data_text),
                    value_type: Some(m.value_type),
                });
            }
        }

        results
    }

    /// Get rough count of cached keys (for determining if cache is populated)
    pub fn cached_key_count(&self) -> usize {
        let conn = match self.open_db() {
            Ok(c) => c,
            Err(_) => return 0,
        };
        conn.query_row("SELECT COUNT(*) FROM keys WHERE NOT deleted", [], |r| r.get(0))
            .unwrap_or(0)
    }

    /// Get count of cached values
    pub fn cached_value_count(&self) -> usize {
        let conn = match self.open_db() {
            Ok(c) => c,
            Err(_) => return 0,
        };
        conn.query_row("SELECT COUNT(*) FROM key_values WHERE NOT deleted", [], |r| r.get(0))
            .unwrap_or(0)
    }
}

// ── Search Types ────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct CachedSearchOptions {
    pub search_keys: bool,
    pub search_value_names: bool,
    pub search_value_data: bool,
    pub case_sensitive: bool,
    pub use_regex: bool,
    pub max_results: usize,
    pub roots: Vec<RootKey>,
    pub value_type_filter: Option<String>,
}

impl Default for CachedSearchOptions {
    fn default() -> Self {
        Self {
            search_keys: true,
            search_value_names: true,
            search_value_data: true,
            case_sensitive: false,
            use_regex: false,
            max_results: 10000,
            roots: Vec::new(),
            value_type_filter: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CachedSearchResult {
    pub root: RootKey,
    pub path: String,
    pub match_type: CachedMatchType,
    pub value_name: Option<String>,
    pub value_data: Option<String>,
    pub value_type: Option<String>,
}

impl CachedSearchResult {
    pub fn full_path(&self) -> String {
        if self.path.is_empty() {
            self.root.to_string()
        } else {
            format!("{}\\{}", self.root, self.path)
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum CachedMatchType {
    KeyName,
    ValueName,
    ValueData,
}

impl std::fmt::Display for CachedMatchType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CachedMatchType::KeyName => write!(f, "Key"),
            CachedMatchType::ValueName => write!(f, "Value Name"),
            CachedMatchType::ValueData => write!(f, "Value Data"),
        }
    }
}

#[derive(Debug, Clone)]
/// A value match result from SQLite cache search.
pub struct CachedValueMatch {
    pub root: RootKey,
    pub path: String,
    pub value_name: String,
    pub value_type: String,
    pub value_data_text: String,
    pub matched_name: bool,
    pub matched_data: bool,
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// DATABASE SCHEMA
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Initializes the SQLite database schema.
///
/// # Tables Overview
///
/// ## `keys` - Cached registry keys
/// ```sql
/// root         -- Registry hive name (e.g., "HKEY_CURRENT_USER")
/// path         -- Full path within hive (e.g., "Software\\MyApp")
/// name_lower   -- Lowercase key name for case-insensitive search
/// parent_path  -- Path of parent key (for tree navigation)
/// registry_lwt -- Last Write Time from registry (for conflict detection)
/// local_lwt    -- When we last modified this locally
/// dirty        -- 1 if modified locally and needs sync, 0 otherwise
/// deleted      -- 1 if marked for deletion, 0 otherwise (soft delete)
/// ```
///
/// ## `key_values` - Cached registry values
/// Similar structure for storing name/type/data of registry values.
///
/// ## `pending_changes` - Queue of changes to sync
/// Stores serialized PendingChange objects as JSON.
///
/// ## `bookmarks` - User's saved locations
/// Stores user bookmarks with names, notes, and colors.
///
/// # Indexes
/// Indexes speed up common queries:
/// - `idx_keys_name_lower`: Fast case-insensitive key search
/// - `idx_keys_parent`: Fast child lookup for tree navigation
/// - `idx_keys_dirty`: Fast query of modified keys
/// - etc.
fn init_sync_schema(conn: &Connection) {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS keys (
            root            TEXT NOT NULL,
            path            TEXT NOT NULL,
            name_lower      TEXT NOT NULL,
            parent_path     TEXT NOT NULL DEFAULT '',
            registry_lwt    INTEGER NOT NULL DEFAULT 0,
            local_lwt       INTEGER NOT NULL DEFAULT 0,
            dirty           INTEGER NOT NULL DEFAULT 0,
            deleted         INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (root, path)
        );

        CREATE TABLE IF NOT EXISTS key_values (
            root             TEXT NOT NULL,
            key_path         TEXT NOT NULL,
            value_name       TEXT NOT NULL,
            value_type       TEXT NOT NULL DEFAULT '',
            value_data       BLOB NOT NULL DEFAULT x'',
            dirty            INTEGER NOT NULL DEFAULT 0,
            deleted          INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (root, key_path, value_name)
        );

        CREATE TABLE IF NOT EXISTS pending_changes (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            change_json TEXT NOT NULL,
            created_at  INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS bookmarks (
            path       TEXT PRIMARY KEY,
            name       TEXT NOT NULL,
            notes      TEXT NOT NULL DEFAULT '',
            color      TEXT,
            sort_order INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS settings (
            key        TEXT PRIMARY KEY,
            value      TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_keys_name_lower ON keys(name_lower);
        CREATE INDEX IF NOT EXISTS idx_keys_parent ON keys(root, parent_path);
        CREATE INDEX IF NOT EXISTS idx_keys_dirty ON keys(dirty) WHERE dirty = 1;
        CREATE INDEX IF NOT EXISTS idx_keys_deleted ON keys(deleted) WHERE deleted = 0;
        CREATE INDEX IF NOT EXISTS idx_keys_root_path ON keys(root, path);
        CREATE INDEX IF NOT EXISTS idx_vals_key ON key_values(root, key_path);
        CREATE INDEX IF NOT EXISTS idx_vals_dirty ON key_values(dirty) WHERE dirty = 1;
        CREATE INDEX IF NOT EXISTS idx_vals_deleted ON key_values(deleted) WHERE deleted = 0;
        CREATE INDEX IF NOT EXISTS idx_vals_name ON key_values(LOWER(value_name));
        CREATE INDEX IF NOT EXISTS idx_vals_name_lower ON key_values(root, key_path, LOWER(value_name));",
    )
    .ok();
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// HELPER FUNCTIONS
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Extracts the parent path from a full registry path.
///
/// # Examples
/// ```
/// parent_path("Software\\Microsoft\\Windows") // returns "Software\\Microsoft"
/// parent_path("Software")                      // returns "" (empty string)
/// ```
///
/// # Rust Concept: String Slicing
/// `path[..pos]` creates a slice (view) of the string from start to `pos`.
/// We then call `.to_string()` to convert the slice into an owned String.
fn parent_path(path: &str) -> String {
    match path.rfind('\\') {  // rfind searches from the right (end)
        Some(pos) => path[..pos].to_string(),  // Found: take everything before
        None => String::new(),                  // Not found: no parent (root level)
    }
}

/// Returns the current Unix timestamp (seconds since 1970).
///
/// # Why Unix Timestamps?
/// They're simple integers, easy to compare, and cross-platform.
/// Windows FILETIME can be converted to/from Unix time as needed.
fn now_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)         // Time since Jan 1, 1970
        .unwrap_or_default()                // Handle pre-1970 times (shouldn't happen)
        .as_secs() as i64                   // Convert to seconds, cast to i64
}

/// Converts a RegValue to a string type and byte array for SQLite storage.
///
/// # Registry Value Types
/// - REG_SZ: Simple string
/// - REG_EXPAND_SZ: String with %VARIABLE% expansion
/// - REG_MULTI_SZ: Multiple strings (null-separated)
/// - REG_DWORD: 32-bit integer (little-endian)
/// - REG_QWORD: 64-bit integer (little-endian)
/// - REG_BINARY: Raw bytes
/// - REG_NONE: No value
///
/// # Return Value
/// Returns a tuple: (type_name, raw_bytes)
fn serialize_reg_value(value: &RegValue) -> (String, Vec<u8>) {
    match value {
        RegValue::String(s) => ("REG_SZ".to_string(), s.as_bytes().to_vec()),
        RegValue::ExpandString(s) => ("REG_EXPAND_SZ".to_string(), s.as_bytes().to_vec()),
        RegValue::MultiString(v) => ("REG_MULTI_SZ".to_string(), v.join("\0").as_bytes().to_vec()),
        RegValue::Dword(d) => ("REG_DWORD".to_string(), d.to_le_bytes().to_vec()),
        RegValue::Qword(q) => ("REG_QWORD".to_string(), q.to_le_bytes().to_vec()),
        RegValue::Binary(b) => ("REG_BINARY".to_string(), b.clone()),
        RegValue::None => ("REG_NONE".to_string(), Vec::new()),
        RegValue::Unknown(ty, data) => (format!("REG_UNKNOWN_{}", ty), data.clone()),
    }
}

/// Converts stored type string and bytes back to a RegValue.
///
/// This is the inverse of `serialize_reg_value`.
///
/// # Rust Concept: try_into()
/// `data.try_into()` attempts to convert a slice to a fixed-size array.
/// For example, `[u8]` to `[u8; 4]` for DWORD.
/// It returns a Result because the slice might not be the right length.
fn deserialize_reg_value(type_str: &str, data: &[u8]) -> RegValue {
    match type_str {
        "REG_SZ" => RegValue::String(String::from_utf8_lossy(data).to_string()),
        "REG_EXPAND_SZ" => RegValue::ExpandString(String::from_utf8_lossy(data).to_string()),
        "REG_MULTI_SZ" => {
            let s = String::from_utf8_lossy(data);
            RegValue::MultiString(s.split('\0').map(|x| x.to_string()).collect())
        }
        "REG_DWORD" => {
            // Try to convert bytes to [u8; 4] array, fallback to zeros
            let arr: [u8; 4] = data.try_into().unwrap_or([0; 4]);
            RegValue::Dword(u32::from_le_bytes(arr))  // Little-endian conversion
        }
        "REG_QWORD" => {
            let arr: [u8; 8] = data.try_into().unwrap_or([0; 8]);
            RegValue::Qword(u64::from_le_bytes(arr))
        }
        "REG_BINARY" => RegValue::Binary(data.to_vec()),
        "REG_NONE" => RegValue::None,
        _ => {
            // Handle unknown types (e.g., "REG_UNKNOWN_99")
            if type_str.starts_with("REG_UNKNOWN_") {
                let ty: u32 = type_str
                    .strip_prefix("REG_UNKNOWN_")  // Remove prefix
                    .and_then(|s| s.parse().ok())  // Try to parse number
                    .unwrap_or(99);                // Default to 99
                RegValue::Unknown(ty, data.to_vec())
            } else {
                // Completely unknown type, treat as binary
                RegValue::Binary(data.to_vec())
            }
        }
    }
}

/// Converts a color name string to a BookmarkColor enum.
///
/// # Why This Function?
/// SQLite stores the color as a string (e.g., "Red").
/// We need to convert back to the enum when loading bookmarks.
fn color_from_str(s: &str) -> Option<BookmarkColor> {
    match s {
        "Red" => Some(BookmarkColor::Red),
        "Green" => Some(BookmarkColor::Green),
        "Blue" => Some(BookmarkColor::Blue),
        "Yellow" => Some(BookmarkColor::Yellow),
        "Purple" => Some(BookmarkColor::Purple),
        "Orange" => Some(BookmarkColor::Orange),
        _ => None,  // Unknown color returns None
    }
}
