// Copyright (c) 2026 Eric Chubb
// Licensed under the MIT License

//! # Registry Search Module
//!
//! This module provides comprehensive search capabilities across the Windows Registry.
//! It supports multiple search strategies and can search through millions of keys quickly.
//!
//! ## Search Strategies
//!
//! The search system has three strategies, used in order of preference:
//!
//! 1. **SQLite Cache Search** (fastest) - Searches our local SQLite database
//! 2. **Index Search** (fast) - Uses a pre-built in-memory index
//! 3. **Live Registry Search** (slowest) - Walks the registry in real-time
//!
//! ## What Can Be Searched
//!
//! - **Key Names**: The name of registry keys (folders)
//! - **Value Names**: Names of values within keys
//! - **Value Data**: The actual data stored in values
//!
//! ## Search Features
//!
//! - Plain text or regex patterns
//! - Case-sensitive or case-insensitive
//! - Parallel processing using rayon
//! - Cancelable at any time
//! - Progress tracking (keys scanned, current path)
//!
//! ## Threading Model
//!
//! All searches run in background threads to keep the UI responsive.
//! State is shared via `Arc` (atomic reference counting) and `Mutex` (locks).
//!
//! ## Example Usage
//!
//! ```rust
//! let options = SearchOptions {
//!     query: "Python".to_string(),
//!     search_keys: true,
//!     search_value_names: true,
//!     search_value_data: false,
//!     ..Default::default()
//! };
//!
//! let state = SearchState::new();
//! start_search(options, state.clone(), None);
//!
//! // Poll for results in UI loop
//! while state.is_searching.load(Ordering::Relaxed) {
//!     let count = state.results.lock().unwrap().len();
//!     println!("Found {} results so far...", count);
//! }
//! ```

use crate::index::RegistryIndex;
use crate::registry::{self, RootKey};
use crate::sync::{CachedMatchType, CachedSearchOptions, SyncStore};
use rayon::prelude::*;
use regex::Regex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// SEARCH RESULT
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// A single search result.
///
/// # Fields
///
/// - `root`: Which root key (HKEY_CURRENT_USER, etc.)
/// - `path`: Path within that root (e.g., "Software\\Microsoft")
/// - `match_type`: What matched (key name, value name, or value data)
/// - `value_name`: For value matches, the name of the matched value
/// - `value_data`: For value matches, the data (truncated for display)
/// - `value_type`: For value matches, the type (REG_SZ, REG_DWORD, etc.)
///
/// # Example
///
/// For a match on `HKEY_CURRENT_USER\Environment\PATH`:
/// ```text
/// SearchResult {
///     root: HkeyCurrentUser,
///     path: "Environment",
///     match_type: ValueName,
///     value_name: Some("PATH"),
///     value_data: Some("C:\\Windows\\System32;..."),
///     value_type: Some("REG_EXPAND_SZ"),
/// }
/// ```
#[derive(Debug, Clone)]
pub struct SearchResult {
    /// Which registry root this result is under
    pub root: RootKey,
    /// Path to the key (relative to root)
    pub path: String,
    /// What kind of match this is
    pub match_type: MatchType,
    /// Name of the matching value (if it's a value match)
    pub value_name: Option<String>,
    /// Display string of the value data (if it's a value match)
    pub value_data: Option<String>,
    /// Registry type of the value (if it's a value match)
    pub value_type: Option<String>,
}

impl SearchResult {
    /// Get the full path including the root key name.
    ///
    /// # Returns
    ///
    /// Full path like "HKEY_CURRENT_USER\\Software\\Microsoft"
    pub fn full_path(&self) -> String {
        if self.path.is_empty() {
            self.root.to_string()
        } else {
            format!("{}\\{}", self.root, self.path)
        }
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// MATCH TYPE
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Indicates what part of the registry matched the search query.
///
/// This helps the UI display context and allows filtering by match type.
#[derive(Debug, Clone, PartialEq)]
pub enum MatchType {
    /// The search query matched the name of a registry key
    KeyName,
    /// The search query matched the name of a value
    ValueName,
    /// The search query matched the data stored in a value
    ValueData,
}

impl std::fmt::Display for MatchType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MatchType::KeyName => write!(f, "Key"),
            MatchType::ValueName => write!(f, "Value Name"),
            MatchType::ValueData => write!(f, "Value Data"),
        }
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// SEARCH OPTIONS
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Configuration for a registry search operation.
///
/// # Design Philosophy
///
/// Search options are immutable once created. This prevents race conditions
/// where options could change mid-search. Clone the options if you need
/// to modify them for a new search.
///
/// # Default Values
///
/// ```rust
/// SearchOptions::default() == SearchOptions {
///     query: "",
///     use_regex: false,
///     case_sensitive: false,
///     search_keys: true,
///     search_value_names: true,
///     search_value_data: true,
///     max_results: 10000,
///     roots_to_search: [all five roots],
///     max_depth: None,  // unlimited
///     value_type_filter: None,  // all types
/// }
/// ```
#[derive(Debug, Clone)]
pub struct SearchOptions {
    /// The search query (plain text or regex pattern)
    pub query: String,
    
    /// If true, `query` is interpreted as a regular expression.
    /// If false, it's a simple substring match.
    pub use_regex: bool,
    
    /// If true, matching is case-sensitive.
    /// If false, "PATH" matches "Path", "path", "pATH", etc.
    pub case_sensitive: bool,
    
    /// Search within registry key names (the "folders")
    pub search_keys: bool,
    
    /// Search within value names (e.g., "Version", "InstallPath")
    pub search_value_names: bool,
    
    /// Search within value data (the actual stored content)
    pub search_value_data: bool,
    
    /// Stop after finding this many results.
    /// Prevents runaway searches and memory exhaustion.
    pub max_results: usize,
    
    /// Which root keys to search. Empty = search nothing.
    pub roots_to_search: Vec<RootKey>,
    
    /// Maximum depth to descend into the tree.
    /// `None` = unlimited (go as deep as possible)
    /// `Some(1)` = only direct children of roots
    pub max_depth: Option<usize>,
    
    /// Filter to only values of a specific type.
    /// Examples: "REG_SZ", "REG_DWORD", "REG_BINARY"
    /// `None` = search all value types
    pub value_type_filter: Option<String>,
}

impl Default for SearchOptions {
    fn default() -> Self {
        Self {
            query: String::new(),
            use_regex: false,
            case_sensitive: false,
            search_keys: true,
            search_value_names: true,
            search_value_data: true,
            max_results: 10000,
            roots_to_search: RootKey::all().to_vec(),
            max_depth: None,
            value_type_filter: None,
        }
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// SEARCH STATE
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Shared state for an in-progress search.
///
/// # Thread Safety
///
/// This struct is designed to be shared between the search thread and the
/// UI thread. All fields use atomic types or mutexes for safe concurrent access.
///
/// # Rust Concept: Arc (Atomic Reference Counting)
///
/// `Arc<T>` is like `Rc<T>` but safe to share across threads.
/// When you clone an `Arc`, it just increments a counter instead of
/// copying the data. When all clones are dropped, the data is freed.
///
/// # Rust Concept: AtomicBool / AtomicU64
///
/// These are primitive values that can be safely read/written from
/// multiple threads without locks. They're faster than mutexes for
/// simple values.
///
/// # Example Usage
///
/// ```rust
/// let state = SearchState::new();
/// let state_clone = state.clone();  // Cheap clone - just increments ref counts
///
/// // In search thread:
/// state_clone.is_searching.store(true, Ordering::SeqCst);
///
/// // In UI thread:
/// if state.is_searching.load(Ordering::Relaxed) {
///     ui.show_progress();
/// }
/// ```
#[derive(Debug, Clone)]
pub struct SearchState {
    /// Accumulated search results (append-only during search)
    pub results: Arc<Mutex<Vec<SearchResult>>>,
    
    /// True while a search is running
    pub is_searching: Arc<AtomicBool>,
    
    /// Set to true to request search cancellation.
    /// The search thread checks this periodically and stops early.
    pub cancel: Arc<AtomicBool>,
    
    /// Number of registry keys scanned so far (for progress display)
    pub keys_scanned: Arc<AtomicU64>,
    
    /// Current key path being scanned (for "Searching: ..." display)
    pub current_path: Arc<Mutex<String>>,
}

impl SearchState {
    /// Create a new search state ready for a search.
    pub fn new() -> Self {
        Self {
            results: Arc::new(Mutex::new(Vec::new())),
            is_searching: Arc::new(AtomicBool::new(false)),
            cancel: Arc::new(AtomicBool::new(false)),
            keys_scanned: Arc::new(AtomicU64::new(0)),
            current_path: Arc::new(Mutex::new(String::new())),
        }
    }

    /// Reset state for a new search.
    ///
    /// Clears all previous results and counters.
    /// Call this before starting a new search to avoid mixing results.
    pub fn reset(&self) {
        self.results.lock().unwrap().clear();
        self.is_searching.store(false, Ordering::SeqCst);
        self.cancel.store(false, Ordering::SeqCst);
        self.keys_scanned.store(0, Ordering::SeqCst);
        *self.current_path.lock().unwrap() = String::new();
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// MATCHER - Internal pattern matching helper
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Internal helper that handles both plain text and regex matching.
///
/// # Why This Exists
///
/// We don't want to check `options.use_regex` on every single match.
/// Instead, we create a `Matcher` once and it handles the matching
/// efficiently.
///
/// # Performance Note
///
/// The regex is compiled once when the Matcher is created.
/// Regex compilation is expensive, but matching is fast.
struct Matcher {
    /// Compiled regex pattern (if regex mode)
    regex: Option<Regex>,
    /// Lowercased query string (if plain text mode)
    plain: String,
    /// Whether to match case-sensitively
    case_sensitive: bool,
}

impl Matcher {
    /// Create a new matcher from search options.
    ///
    /// # Returns
    /// - `Ok(Matcher)` - Ready to match
    /// - `Err(String)` - Invalid regex pattern
    fn new(options: &SearchOptions) -> Result<Self, String> {
        if options.use_regex {
            // Build regex with optional case-insensitivity flag
            let pattern = if options.case_sensitive {
                options.query.clone()
            } else {
                // (?i) is the case-insensitive flag in regex
                format!("(?i){}", options.query)
            };
            let regex = Regex::new(&pattern).map_err(|e| format!("Invalid regex: {}", e))?;
            Ok(Self {
                regex: Some(regex),
                plain: String::new(),
                case_sensitive: options.case_sensitive,
            })
        } else {
            // For plain text, pre-lowercase the query for faster matching
            let plain = if options.case_sensitive {
                options.query.clone()
            } else {
                options.query.to_lowercase()
            };
            Ok(Self {
                regex: None,
                plain,
                case_sensitive: options.case_sensitive,
            })
        }
    }

    /// Check if the given text matches the search pattern.
    ///
    /// # Arguments
    /// * `text` - The text to search within
    ///
    /// # Returns
    /// `true` if the text contains the pattern
    fn matches(&self, text: &str) -> bool {
        if let Some(ref regex) = self.regex {
            // Regex match
            regex.is_match(text)
        } else if self.case_sensitive {
            // Case-sensitive substring match
            text.contains(&self.plain)
        } else {
            // Case-insensitive substring match
            text.to_lowercase().contains(&self.plain)
        }
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// PUBLIC SEARCH FUNCTIONS
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Start a background search operation.
///
/// # How It Works
///
/// 1. Resets the search state
/// 2. Spawns a background thread
/// 3. Tries to use the index if available
/// 4. Falls back to live registry scanning
///
/// # Arguments
///
/// * `options` - Search configuration (query, filters, etc.)
/// * `state` - Shared state for results and progress
/// * `index` - Optional pre-built index for fast searching
///
/// # Thread Safety
///
/// This function returns immediately. The actual search runs in a
/// background thread. Use `state.is_searching` to check if it's done.
///
/// # Cancellation
///
/// To cancel the search, set `state.cancel.store(true, Ordering::SeqCst)`.
/// The search will stop as soon as it notices the flag.
pub fn start_search(options: SearchOptions, state: SearchState, index: Option<RegistryIndex>) {
    // Reset clears previous results and flags
    state.reset();
    // Mark that we're now searching
    state.is_searching.store(true, Ordering::SeqCst);

    // Clone state for the background thread
    // (Arc clone is cheap - just increments reference count)
    let state_clone = state.clone();
    
    // Spawn a background thread to do the actual work
    std::thread::spawn(move || {
        // Try the index first — if it has data and covers what we need, use it
        let used_index = if let Some(ref idx) = index {
            try_indexed_search(&options, &state_clone, idx)
        } else {
            false
        };

        // Fall back to live scan if index didn't cover it
        if !used_index {
            run_search(options, state_clone);
        }
    });
}

/// Start a search using the SQLite store first, falling back to live registry if needed.
///
/// # When To Use This
///
/// Use this variant when you have a `SyncStore` with cached data.
/// It's much faster than live searching because SQLite can use indexes.
///
/// # Fallback Behavior
///
/// If `fallback_to_live` is true and SQLite search finds nothing,
/// it will automatically fall back to a live registry scan.
///
/// # Limitations
///
/// SQLite search doesn't support regex patterns. If `options.use_regex`
/// is true, it goes straight to live search.
pub fn start_search_with_store(
    options: SearchOptions,
    state: SearchState,
    store: SyncStore,
    fallback_to_live: bool,
) {
    state.reset();
    state.is_searching.store(true, Ordering::SeqCst);

    let state_clone = state.clone();
    std::thread::spawn(move || {
        // Check if we have enough cached data to answer from SQLite
        let cached_keys = store.cached_key_count();
        let _cached_values = store.cached_value_count();

        // If we have substantial cached data, use SQLite search
        // Threshold: at least 1000 keys cached
        let use_sqlite = cached_keys >= 1000;

        if use_sqlite {
            try_sqlite_search(&options, &state_clone, &store);
        }

        // If SQLite search didn't find much or was skipped, and fallback is enabled
        let result_count = state_clone.results.lock().unwrap().len();
        if result_count == 0 && fallback_to_live && !state_clone.cancel.load(Ordering::Relaxed) {
            // Fall back to live registry scan
            run_search(options, state_clone);
        } else {
            state_clone.is_searching.store(false, Ordering::SeqCst);
        }
    });
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// SEARCH IMPLEMENTATIONS
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Search using the SQLite cache via SyncStore.
///
/// # How It Works
///
/// Uses SQL LIKE queries to search the cached registry data.
/// Much faster than live search because:
/// - No Windows API calls
/// - SQLite can use indexes
/// - Data is already organized
///
/// # Limitations
///
/// - Doesn't support regex (SQLite LIKE is limited)
/// - Only searches cached data (may be stale or incomplete)
fn try_sqlite_search(options: &SearchOptions, state: &SearchState, store: &SyncStore) {
    // Skip regex searches - SQLite LIKE doesn't support regex
    // Fall back to live search for those
    if options.use_regex {
        return;
    }

    // Convert our options to the format SyncStore expects
    let cached_options = CachedSearchOptions {
        search_keys: options.search_keys,
        search_value_names: options.search_value_names,
        search_value_data: options.search_value_data,
        case_sensitive: options.case_sensitive,
        max_results: options.max_results,
        roots: options.roots_to_search.clone(),
        value_type_filter: options.value_type_filter.clone(),
    };

    // Execute the search against the cache
    let cached_results = store.search(&options.query, &cached_options);

    // Convert results to our SearchResult format and add to state
    let mut results = state.results.lock().unwrap();
    for cr in cached_results {
        // Check for cancellation or max results limit
        if state.cancel.load(Ordering::Relaxed) || results.len() >= options.max_results {
            break;
        }

        // Convert the cached result to a SearchResult
        results.push(SearchResult {
            root: cr.root,
            path: cr.path,
            match_type: match cr.match_type {
                CachedMatchType::KeyName => MatchType::KeyName,
                CachedMatchType::ValueName => MatchType::ValueName,
                CachedMatchType::ValueData => MatchType::ValueData,
            },
            value_name: cr.value_name,
            value_data: cr.value_data,
            value_type: cr.value_type,
        });
    }

    // Update progress counter with result count
    state
        .keys_scanned
        .store(results.len() as u64, Ordering::SeqCst);
}

/// Attempt to answer the search entirely from the in-memory index.
///
/// # When Index Can Be Used
///
/// The index can answer a search if:
/// 1. The index exists and has data
/// 2. The index covers all the root keys the user wants to search
/// 3. The index depth >= the user's max_depth (if specified)
/// 4. Value searching is only requested if values were indexed
///
/// # Returns
///
/// * `true` - Search was completed from index
/// * `false` - Index couldn't be used, caller should use live search
fn try_indexed_search(options: &SearchOptions, state: &SearchState, index: &RegistryIndex) -> bool {
    // Check if we have any indexed data
    if !index.has_index() {
        return false;
    }

    // Check if the index covers the roots we want to search
    let indexed_roots = index.roots.lock().unwrap().clone();
    let all_covered = options
        .roots_to_search
        .iter()
        .all(|r| indexed_roots.contains(r));
    if !all_covered {
        return false;
    }

    // If user wants a deeper search than our index depth, can't use index alone
    if let Some(user_depth) = options.max_depth {
        let idx_depth = index.max_depth.lock().unwrap();
        if let Some(id) = *idx_depth {
            if user_depth > id {
                return false;
            }
        }
    }

    let max = options.max_results;

    // Search keys from index
    if options.search_keys {
        let key_results = index.search_keys(&options.query, options.case_sensitive, options.use_regex);
        let mut results = state.results.lock().unwrap();
        for k in key_results {
            if results.len() >= max || state.cancel.load(Ordering::Relaxed) {
                break;
            }
            if !options.roots_to_search.contains(&k.root) {
                continue;
            }
            results.push(SearchResult {
                root: k.root,
                path: k.path,
                match_type: MatchType::KeyName,
                value_name: None,
                value_data: None,
                value_type: None,
            });
        }
    }

    // Search values from index
    if (options.search_value_names || options.search_value_data)
        && index.index_values.load(Ordering::Relaxed)
    {
        let value_results = index.search_values(
            &options.query,
            options.case_sensitive,
            options.use_regex,
            options.search_value_names,
            options.search_value_data,
            options.value_type_filter.as_deref(),
        );
        let mut results = state.results.lock().unwrap();
        for v in value_results {
            if results.len() >= max || state.cancel.load(Ordering::Relaxed) {
                break;
            }
            if !options.roots_to_search.contains(&v.root) {
                continue;
            }
            let match_type = if options.search_value_names
                && v.value_name_lower.contains(&options.query.to_lowercase())
            {
                MatchType::ValueName
            } else {
                MatchType::ValueData
            };
            results.push(SearchResult {
                root: v.root,
                path: v.key_path,
                match_type,
                value_name: Some(v.value_name),
                value_data: Some(v.value_data_text),
                value_type: Some(v.value_type),
            });
        }
    } else if options.search_value_names || options.search_value_data {
        // Index doesn't have values indexed — can't satisfy this from index alone
        state.is_searching.store(false, Ordering::SeqCst);
        return false;
    }

    let count = state.results.lock().unwrap().len();
    state.keys_scanned.store(count as u64, Ordering::SeqCst);
    state.is_searching.store(false, Ordering::SeqCst);
    true
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// LIVE REGISTRY SEARCH
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Run a live search directly against the Windows Registry.
///
/// # Performance Characteristics
///
/// This is the slowest search method but the most thorough. It:
/// - Makes Windows API calls for every key/value
/// - Can take minutes for a full registry search
/// - Uses parallel processing to speed things up
///
/// # Parallel Strategy (using rayon)
///
/// The Windows Registry is tree-structured. We get the top-level
/// subkeys of each root, then process those branches in parallel.
///
/// Example for HKEY_CURRENT_USER:
/// ```text
/// HKEY_CURRENT_USER
/// ├── AppEvents    ← Thread 1 processes this entire branch
/// ├── Console      ← Thread 2 processes this branch
/// ├── Environment  ← Thread 3 processes this branch
/// ├── Software     ← Thread 4 processes this (huge) branch
/// └── ...
/// ```
///
/// # Error Handling
///
/// If the regex pattern is invalid, we add an error result and stop.
/// Permission errors on individual keys are silently skipped.
fn run_search(options: SearchOptions, state: SearchState) {
    // Create matcher (validates regex pattern)
    let matcher = match Matcher::new(&options) {
        Ok(m) => Arc::new(m),  // Wrap in Arc for thread-safe sharing
        Err(e) => {
            // Invalid regex - report error via results
            let mut results = state.results.lock().unwrap();
            results.push(SearchResult {
                root: RootKey::HkeyCurrentUser,
                path: String::new(),
                match_type: MatchType::KeyName,
                value_name: Some(format!("Error: {}", e)),
                value_data: None,
                value_type: None,
            });
            state.is_searching.store(false, Ordering::SeqCst);
            return;
        }
    };

    // Collect top-level subkeys from all roots to search in parallel
    // This creates "work items" that can be distributed across threads
    let mut work_items: Vec<(RootKey, String)> = Vec::new();
    for root in &options.roots_to_search {
        if state.cancel.load(Ordering::Relaxed) {
            break;
        }

        // Check root name itself
        if options.search_keys && matcher.matches(&root.to_string()) {
            let mut results = state.results.lock().unwrap();
            results.push(SearchResult {
                root: root.clone(),
                path: String::new(),
                match_type: MatchType::KeyName,
                value_name: None,
                value_data: None,
                value_type: None,
            });
        }

        // Get top-level subkeys for parallel processing
        if let Ok(subkeys) = registry::enumerate_subkeys(root, "") {
            for subkey in subkeys {
                work_items.push((root.clone(), subkey));
            }
        }
    }

    // Process top-level branches in parallel using rayon
    // par_iter() automatically distributes work across available CPU cores
    work_items.par_iter().for_each(|(root, subkey)| {
        // Early exit if cancelled
        if state.cancel.load(Ordering::Relaxed) {
            return;
        }
        // Early exit if we have enough results
        let max = options.max_results;
        if state.results.lock().unwrap().len() >= max {
            return;
        }
        // Recursively search this branch
        search_key_recursive(root, subkey, &options, &matcher, &state, 1);
    });

    // Mark search as complete
    state.is_searching.store(false, Ordering::SeqCst);
}

/// Recursively search a registry key and its descendants.
///
/// # Algorithm
///
/// For each key:
/// 1. Check if the key name matches
/// 2. Check all values in the key
/// 3. Recurse into each subkey
///
/// # Performance Notes
///
/// - Checks cancellation flag frequently
/// - Updates progress every 500 keys (not every key - that's too slow)
/// - Stops early if max_results is reached
///
/// # Arguments
///
/// * `root` - Which root key we're under
/// * `path` - Current path relative to root
/// * `options` - Search configuration
/// * `matcher` - Pre-built pattern matcher
/// * `state` - Shared state for results and progress
/// * `depth` - Current depth (for max_depth checking)
fn search_key_recursive(
    root: &RootKey,
    path: &str,
    options: &SearchOptions,
    matcher: &Matcher,
    state: &SearchState,
    depth: usize,
) {
    // Check for cancellation
    if state.cancel.load(Ordering::Relaxed) {
        return;
    }

    // Check depth limit
    if let Some(max_depth) = options.max_depth {
        if depth > max_depth {
            return;
        }
    }

    // Check result limit
    if state.results.lock().unwrap().len() >= options.max_results {
        return;
    }

    // Update progress counter
    state.keys_scanned.fetch_add(1, Ordering::Relaxed);

    // Update current path display periodically
    // We don't do this every key because lock contention would slow us down
    if state.keys_scanned.load(Ordering::Relaxed) % 500 == 0 {
        if let Ok(mut current) = state.current_path.lock() {
            *current = format!("{}\\{}", root, path);
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    // Check key name
    // ─────────────────────────────────────────────────────────────────────
    if options.search_keys {
        // Extract just the key name (last component of path)
        let key_name = path.rsplit('\\').next().unwrap_or(path);
        if matcher.matches(key_name) {
            let mut results = state.results.lock().unwrap();
            if results.len() < options.max_results {
                results.push(SearchResult {
                    root: root.clone(),
                    path: path.to_string(),
                    match_type: MatchType::KeyName,
                    value_name: None,
                    value_data: None,
                    value_type: None,
                });
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    // Check values in this key
    // ─────────────────────────────────────────────────────────────────────
    if options.search_value_names || options.search_value_data {
        if let Ok(values) = registry::enumerate_values(root, path) {
            for val in &values {
                // Check cancellation frequently when iterating values
                if state.cancel.load(Ordering::Relaxed) {
                    return;
                }

                // Filter by value type if specified
                if let Some(ref type_filter) = options.value_type_filter {
                    if val.data.type_name() != type_filter {
                        continue;
                    }
                }

                // Check if name matches
                let name_match =
                    options.search_value_names && matcher.matches(&val.name);
                // Check if data matches
                let data_match = options.search_value_data
                    && matcher.matches(&val.data.searchable_text());

                if name_match || data_match {
                    let mut results = state.results.lock().unwrap();
                    if results.len() < options.max_results {
                        results.push(SearchResult {
                            root: root.clone(),
                            path: path.to_string(),
                            // Prefer value name match over data match for match_type
                            match_type: if name_match {
                                MatchType::ValueName
                            } else {
                                MatchType::ValueData
                            },
                            // Display "(Default)" for the unnamed default value
                            value_name: Some(if val.name.is_empty() {
                                "(Default)".to_string()
                            } else {
                                val.name.clone()
                            }),
                            value_data: Some(val.data.display_data()),
                            value_type: Some(val.data.type_name().to_string()),
                        });
                    }
                }
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    // Recurse into subkeys
    // ─────────────────────────────────────────────────────────────────────
    if let Ok(subkeys) = registry::enumerate_subkeys(root, path) {
        for subkey in subkeys {
            // Check cancellation before each recursive call
            if state.cancel.load(Ordering::Relaxed) {
                return;
            }
            
            // Build child path
            let child_path = if path.is_empty() {
                subkey
            } else {
                format!("{}\\{}", path, subkey)
            };
            
            // Recurse (depth + 1)
            search_key_recursive(root, &child_path, options, matcher, state, depth + 1);
        }
    }
}
