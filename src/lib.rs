// Copyright (c) 2026 Eric Chubb
// Licensed under the MIT License

//! # Rust Hive
//!
//! A Rust library for Windows Registry access with SQLite caching.
//!
//! ## Features
//!
//! - **Safe Registry Access**: Read and write Windows Registry values
//! - **SQLite Caching**: Cache registry data locally for faster access
//! - **Change Tracking**: Track pending changes before committing
//! - **Bookmarks**: Save frequently accessed registry keys
//! - **Search**: Full-text search across cached registry data
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use rust_hive::{SyncStore, RootKey};
//!
//! // Create a new store (initializes SQLite database)
//! let store = SyncStore::new();
//!
//! // Pull current state from registry
//! store.pull_from_registry();
//!
//! // Read values from a key
//! if let Some(values) = store.get_values(&RootKey::HkeyCurrentUser, "Software\\MyApp") {
//!     for value in values {
//!         println!("{}: {:?}", value.name, value.data);
//!     }
//! }
//! ```
//!
//! ## Architecture
//!
//! Rust Hive uses a "SQLite-first" architecture:
//!
//! 1. **Read**: Registry data is cached in a local SQLite database
//! 2. **Edit**: Changes are written to SQLite first (staged)
//! 3. **Push**: Staged changes are applied to the actual registry
//! 4. **Pull**: Registry is re-read to sync the cache

pub mod bookmarks;
pub mod index;
pub mod registry;
pub mod search;
pub mod sync;

// Re-export main types at crate root for convenience
pub use bookmarks::{Bookmark, BookmarkColor};
pub use index::RegistryIndex;
pub use registry::{RegValue, RegistryValue, RootKey};
pub use search::{SearchOptions, SearchResult, SearchState};
pub use sync::{DebugCategory, DebugEvent, PendingChange, SyncConflict, SyncStore};
