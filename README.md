# Rust Hive

A Rust library for Windows Registry access with SQLite caching.

## Features

- **Safe Registry Access**: Read and write Windows Registry values with proper error handling
- **SQLite Caching**: Cache registry data locally for faster access and offline viewing
- **Change Tracking**: Track pending changes before committing to the registry
- **Bookmarks**: Save and organize frequently accessed registry keys
- **Search**: Full-text search across cached registry data

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
rust-hive = "0.1"
```

## Quick Start

```rust
use rust_hive::{SyncStore, RootKey, RegistryValue, ValueData};

fn main() {
    // Create a new store (initializes SQLite database)
    let store = SyncStore::new();
    
    // Pull current state from registry
    store.pull_from_registry();
    
    // Read values from a key
    if let Some(values) = store.get_values(&RootKey::CurrentUser, "Software\\MyApp") {
        for value in values {
            println!("{}: {:?}", value.name, value.data);
        }
    }
    
    // Create a new value (staged in SQLite)
    store.set_value(
        &RootKey::CurrentUser,
        "Software\\MyApp",
        "MySetting",
        ValueData::String("Hello".to_string()),
    );
    
    // Push changes to the actual registry
    let conflicts = store.push_to_registry();
    if conflicts.is_empty() {
        println!("Changes applied successfully!");
    }
}
```

## Architecture

Rust Hive uses a "SQLite-first" architecture:

1. **Read**: Registry data is cached in a local SQLite database
2. **Edit**: Changes are written to SQLite first (staged)
3. **Push**: Staged changes are applied to the actual registry
4. **Pull**: Registry is re-read to sync the cache

This approach allows:
- Preview changes before applying
- Batch multiple edits
- Detect conflicts with external changes
- Work offline with cached data

## Modules

- `registry` - Low-level Windows Registry API wrapper
- `sync` - SQLite storage and synchronization logic
- `search` - Search functionality
- `bookmarks` - Bookmark management

## License

MIT License - see [LICENSE](LICENSE) for details.

## Author

Eric Chubb
