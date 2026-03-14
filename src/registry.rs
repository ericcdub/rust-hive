// Copyright (c) 2026 Eric Chubb
// Licensed under the MIT License

//! # Windows Registry API Wrapper
//!
//! This module provides a safe, Rust-friendly interface to the Windows Registry.
//! It wraps the `winreg` crate with higher-level types and error handling.
//!
//! ## Key Concepts
//!
//! ### Registry Structure
//!
//! The Windows Registry is a hierarchical database storing configuration:
//!
//! ```text
//! HKEY_CURRENT_USER              ← Root Key (one of 5)
//! └── Software                   ← Key (like a folder)
//!     └── Microsoft              ← Subkey
//!         ├── (Default) = ""     ← Default Value
//!         ├── Version = "10.0"   ← Named Value (REG_SZ)
//!         └── Build = 0x4A45     ← Named Value (REG_DWORD)
//! ```
//!
//! ### Value Types
//!
//! | Type | Constant | Rust Type |
//! |------|----------|-----------|
//! | String | REG_SZ | `String` |
//! | Expandable String | REG_EXPAND_SZ | `String` |
//! | Multi-String | REG_MULTI_SZ | `Vec<String>` |
//! | 32-bit Integer | REG_DWORD | `u32` |
//! | 64-bit Integer | REG_QWORD | `u64` |
//! | Binary Data | REG_BINARY | `Vec<u8>` |
//!
//! ## Safety
//!
//! All functions in this module are safe Rust. Registry operations that fail
//! return `Result<T, String>` with a descriptive error message.
//!
//! ## Example
//!
//! ```rust,no_run
//! use crate::registry::{RootKey, enumerate_subkeys, enumerate_values};
//!
//! // List subkeys under HKCU\Software
//! let subkeys = enumerate_subkeys(&RootKey::HkeyCurrentUser, "Software")?;
//! for key in subkeys {
//!     println!("Found: {}", key);
//! }
//!
//! // Read values from a key
//! let values = enumerate_values(&RootKey::HkeyCurrentUser, "Environment")?;
//! for val in values {
//!     println!("{} = {}", val.name, val.data);
//! }
//! ```

use std::fmt;
use winreg::enums::*;
use winreg::types::FromRegValue;
use winreg::RegKey;

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// ROOT KEY - The five registry hives
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// The five standard Windows Registry root keys (hives).
///
/// # Root Key Details
///
/// | Key | Purpose | Typical Use |
/// |-----|---------|-------------|
/// | HKEY_CLASSES_ROOT | File associations, COM | Rarely edited directly |
/// | HKEY_CURRENT_USER | Current user settings | Most common for apps |
/// | HKEY_LOCAL_MACHINE | System-wide settings | Requires admin rights |
/// | HKEY_USERS | All user profiles | Rarely used directly |
/// | HKEY_CURRENT_CONFIG | Current hardware profile | Hardware settings |
///
/// # Example
///
/// ```rust
/// use crate::registry::RootKey;
///
/// // Iterate all root keys
/// for root in RootKey::all() {
///     println!("{}", root);  // "HKEY_CLASSES_ROOT", etc.
/// }
///
/// // Parse from string
/// let root = RootKey::from_name("HKEY_CURRENT_USER").unwrap();
/// assert_eq!(root, RootKey::HkeyCurrentUser);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RootKey {
    HkeyClassesRoot,
    HkeyCurrentUser,
    HkeyLocalMachine,
    HkeyUsers,
    HkeyCurrentConfig,
}

impl RootKey {
    /// Returns all five root keys in standard order.
    pub fn all() -> &'static [RootKey] {
        &[
            RootKey::HkeyClassesRoot,
            RootKey::HkeyCurrentUser,
            RootKey::HkeyLocalMachine,
            RootKey::HkeyUsers,
            RootKey::HkeyCurrentConfig,
        ]
    }

    /// Returns the Windows HKEY handle value for this root key.
    ///
    /// # Windows API Note
    /// These are predefined handle values that don't need to be opened/closed.
    pub fn hkey(&self) -> isize {
        match self {
            RootKey::HkeyClassesRoot => HKEY_CLASSES_ROOT.to_owned(),
            RootKey::HkeyCurrentUser => HKEY_CURRENT_USER.to_owned(),
            RootKey::HkeyLocalMachine => HKEY_LOCAL_MACHINE.to_owned(),
            RootKey::HkeyUsers => HKEY_USERS.to_owned(),
            RootKey::HkeyCurrentConfig => HKEY_CURRENT_CONFIG.to_owned(),
        }
    }

    /// Returns a `winreg::RegKey` handle for this root key.
    ///
    /// This is the entry point for all registry operations.
    pub fn reg_key(&self) -> RegKey {
        RegKey::predef(self.hkey())
    }

    /// Parses a root key from its full string name.
    ///
    /// # Arguments
    /// * `name` - Full name like "HKEY_CURRENT_USER"
    ///
    /// # Returns
    /// `Some(RootKey)` if valid, `None` otherwise.
    pub fn from_name(name: &str) -> Option<RootKey> {
        match name {
            "HKEY_CLASSES_ROOT" => Some(RootKey::HkeyClassesRoot),
            "HKEY_CURRENT_USER" => Some(RootKey::HkeyCurrentUser),
            "HKEY_LOCAL_MACHINE" => Some(RootKey::HkeyLocalMachine),
            "HKEY_USERS" => Some(RootKey::HkeyUsers),
            "HKEY_CURRENT_CONFIG" => Some(RootKey::HkeyCurrentConfig),
            _ => None,
        }
    }
}

impl fmt::Display for RootKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RootKey::HkeyClassesRoot => write!(f, "HKEY_CLASSES_ROOT"),
            RootKey::HkeyCurrentUser => write!(f, "HKEY_CURRENT_USER"),
            RootKey::HkeyLocalMachine => write!(f, "HKEY_LOCAL_MACHINE"),
            RootKey::HkeyUsers => write!(f, "HKEY_USERS"),
            RootKey::HkeyCurrentConfig => write!(f, "HKEY_CURRENT_CONFIG"),
        }
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// REG VALUE - Typed registry data
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// The different types of data that can be stored in a registry value.
///
/// # Type Details
///
/// | Variant | Registry Type | Description |
/// |---------|---------------|-------------|
/// | `String` | REG_SZ | Null-terminated string |
/// | `ExpandString` | REG_EXPAND_SZ | String with %VARIABLE% expansion |
/// | `MultiString` | REG_MULTI_SZ | Array of strings |
/// | `Dword` | REG_DWORD | 32-bit unsigned integer |
/// | `Qword` | REG_QWORD | 64-bit unsigned integer |
/// | `Binary` | REG_BINARY | Raw binary data |
/// | `None` | REG_NONE | No defined type |
/// | `Unknown` | Other | Unrecognized type with raw data |
///
/// # Example
///
/// ```rust
/// use crate::registry::RegValue;
///
/// let string_val = RegValue::String("Hello".to_string());
/// let dword_val = RegValue::Dword(42);
///
/// println!("{}", string_val.display_data());  // "Hello"
/// println!("{}", dword_val.display_data());   // "0x0000002a (42)"
/// ```
#[derive(Debug, Clone, PartialEq)]
pub enum RegValue {
    /// REG_SZ - A null-terminated string
    String(String),
    /// REG_EXPAND_SZ - String with unexpanded environment variables (%PATH%, etc.)
    ExpandString(String),
    /// REG_MULTI_SZ - Array of null-terminated strings
    MultiString(Vec<String>),
    /// REG_DWORD - 32-bit unsigned integer (little-endian)
    Dword(u32),
    /// REG_QWORD - 64-bit unsigned integer (little-endian)
    Qword(u64),
    /// REG_BINARY - Raw binary data
    Binary(Vec<u8>),
    /// REG_NONE - No defined type (empty binary)
    None,
    /// Unknown registry type with raw byte data
    Unknown(u32, Vec<u8>),
}

impl RegValue {
    /// Returns the Windows registry type name.
    pub fn type_name(&self) -> &'static str {
        match self {
            RegValue::String(_) => "REG_SZ",
            RegValue::ExpandString(_) => "REG_EXPAND_SZ",
            RegValue::MultiString(_) => "REG_MULTI_SZ",
            RegValue::Dword(_) => "REG_DWORD",
            RegValue::Qword(_) => "REG_QWORD",
            RegValue::Binary(_) => "REG_BINARY",
            RegValue::None => "REG_NONE",
            RegValue::Unknown(ty, _) => {
                match *ty {
                    0 => "REG_NONE",
                    _ => "REG_UNKNOWN",
                }
            }
        }
    }

    /// Returns a human-readable representation of the data.
    ///
    /// For binary data, shows a hex dump truncated to 64 bytes.
    /// For integers, shows both hex and decimal.
    pub fn display_data(&self) -> String {
        match self {
            RegValue::String(s) | RegValue::ExpandString(s) => s.clone(),
            RegValue::MultiString(v) => v.join(" | "),
            RegValue::Dword(d) => format!("0x{:08x} ({})", d, d),
            RegValue::Qword(q) => format!("0x{:016x} ({})", q, q),
            RegValue::Binary(b) => {
                // Show hex dump, truncated to 64 bytes
                b.iter()
                    .take(64)
                    .map(|byte| format!("{:02x}", byte))
                    .collect::<Vec<_>>()
                    .join(" ")
                    + if b.len() > 64 { " ..." } else { "" }
            }
            RegValue::None => "(zero-length binary value)".to_string(),
            RegValue::Unknown(ty, data) => format!("(type {}, {} bytes)", ty, data.len()),
        }
    }

    /// Returns text suitable for searching.
    ///
    /// Includes full binary data and both decimal/hex for numbers.
    pub fn searchable_text(&self) -> String {
        match self {
            RegValue::String(s) | RegValue::ExpandString(s) => s.clone(),
            RegValue::MultiString(v) => v.join(" "),
            RegValue::Dword(d) => format!("{} 0x{:08x}", d, d),
            RegValue::Qword(q) => format!("{} 0x{:016x}", q, q),
            RegValue::Binary(b) => {
                b.iter()
                    .map(|byte| format!("{:02x}", byte))
                    .collect::<Vec<_>>()
                    .join(" ")
            }
            RegValue::None => String::new(),
            RegValue::Unknown(_, data) => {
                data.iter()
                    .map(|byte| format!("{:02x}", byte))
                    .collect::<Vec<_>>()
                    .join(" ")
            }
        }
    }
}

impl fmt::Display for RegValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.display_data())
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// REGISTRY VALUE - A named value entry
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// A registry value with its name and typed data.
///
/// # The Default Value
///
/// Every registry key has a "default" value with an empty name.
/// In the registry editor UI, this appears as "(Default)".
#[derive(Debug, Clone)]
pub struct RegistryValue {
    /// Value name (empty string = default value)
    pub name: String,
    /// The typed data
    pub data: RegValue,
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// CONVERSION FUNCTIONS
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Converts a `winreg::RegValue` to our `RegValue` type.
///
/// # Arguments
/// * `val` - A value from the winreg crate
///
/// # Returns
/// Our `RegValue` enum with properly typed data.
pub fn from_winreg_value(val: &winreg::RegValue) -> RegValue {
    match val.vtype {
        REG_SZ => {
            let s: String = String::from_reg_value(val).unwrap_or_default();
            RegValue::String(s)
        }
        REG_EXPAND_SZ => {
            let s: String = String::from_reg_value(val).unwrap_or_default();
            RegValue::ExpandString(s)
        }
        REG_MULTI_SZ => {
            // Parse multi-string: null-separated, double-null terminated
            let bytes = &val.bytes;
            let mut strings = Vec::new();
            let mut current = Vec::new();
            let wide: Vec<u16> = bytes
                .chunks_exact(2)
                .map(|c| u16::from_le_bytes([c[0], c[1]]))
                .collect();
            for &ch in &wide {
                if ch == 0 {
                    if !current.is_empty() {
                        strings.push(String::from_utf16_lossy(&current));
                        current.clear();
                    }
                } else {
                    current.push(ch);
                }
            }
            if !current.is_empty() {
                strings.push(String::from_utf16_lossy(&current));
            }
            RegValue::MultiString(strings)
        }
        REG_DWORD => {
            let d: u32 = u32::from_reg_value(val).unwrap_or(0);
            RegValue::Dword(d)
        }
        REG_QWORD => {
            let q: u64 = u64::from_reg_value(val).unwrap_or(0);
            RegValue::Qword(q)
        }
        REG_BINARY => RegValue::Binary(val.bytes.clone()),
        REG_NONE => RegValue::None,
        _ => {
            let type_id = match val.vtype {
                REG_NONE => 0u32,
                REG_SZ => 1,
                REG_EXPAND_SZ => 2,
                REG_BINARY => 3,
                REG_DWORD => 4,
                REG_MULTI_SZ => 7,
                REG_QWORD => 11,
                _ => 99,
            };
            RegValue::Unknown(type_id, val.bytes.clone())
        }
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// REGISTRY INFORMATION FUNCTIONS
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Get the last write time for a registry key as a comparable u64 timestamp.
///
/// # What This Does
///
/// Every registry key tracks when it was last modified. This function retrieves
/// that timestamp and packs it into a u64 for easy comparison.
///
/// # Why This Matters
///
/// We use last-write-time to detect when another program has modified a key.
/// This enables conflict detection: if the registry changed while we had
/// pending edits, we know there might be a conflict.
///
/// # Timestamp Format
///
/// The u64 is packed as: YYYYY.MM.DD.HH.MM.SS (bit-shifted)
/// This format allows simple numeric comparison:
/// - Higher value = more recent
/// - Easy to sort chronologically
///
/// # Arguments
/// * `root` - Which root key (HKEY_CURRENT_USER, etc.)
/// * `path` - Path to the key (e.g., "Software\\MyApp")
///
/// # Returns
/// * `Some(u64)` - The packed timestamp
/// * `None` - If the key doesn't exist or can't be read
///
/// # Example
///
/// ```rust
/// let old_time = get_last_write_time(&RootKey::HkeyCurrentUser, "Software\\MyApp");
/// // ... user makes changes in regedit ...
/// let new_time = get_last_write_time(&RootKey::HkeyCurrentUser, "Software\\MyApp");
/// if new_time > old_time {
///     println!("Key was modified externally!");
/// }
/// ```
pub fn get_last_write_time(root: &RootKey, path: &str) -> Option<u64> {
    // Get a handle to the root key
    let reg_key = root.reg_key();
    
    // Open the specific subkey (or use root if path is empty)
    let key = if path.is_empty() {
        reg_key
    } else {
        // Use KEY_READ to get info without needing write access
        reg_key.open_subkey_with_flags(path, KEY_READ).ok()?
    };
    
    // Query the key's metadata
    let info = key.query_info().ok()?;
    
    // Get the Windows SYSTEMTIME structure with last write time
    let ft = info.get_last_write_time_system();
    
    // Pack the timestamp into a u64 for easy comparison
    // Bit layout: [year:24][month:8][day:8][hour:8][min:8][sec:8]
    Some(
        (ft.wYear as u64) << 40
            | (ft.wMonth as u64) << 32
            | (ft.wDay as u64) << 24
            | (ft.wHour as u64) << 16
            | (ft.wMinute as u64) << 8
            | (ft.wSecond as u64),
    )
}

/// Check if a registry key exists and is accessible.
///
/// # Purpose
///
/// Before attempting operations on a key, we often need to verify it exists.
/// A key might have been deleted by another program, or we might not have
/// permission to access it.
///
/// # Arguments
/// * `root` - The root key to search under
/// * `path` - Path to check (empty string = the root key itself)
///
/// # Returns
/// * `true` - Key exists and we can read it
/// * `false` - Key doesn't exist, was deleted, or we lack permissions
///
/// # Example
///
/// ```rust
/// // Check before navigating to a key
/// if key_exists(&RootKey::HkeyCurrentUser, "Software\\MyApp") {
///     // Safe to enumerate or read values
/// } else {
///     // Key was deleted or doesn't exist
/// }
/// ```
pub fn key_exists(root: &RootKey, path: &str) -> bool {
    // Root keys always exist
    if path.is_empty() {
        return true;
    }
    
    // Try to open with read access
    // Success = key exists and is accessible
    // Failure = key doesn't exist or access denied
    root.reg_key()
        .open_subkey_with_flags(path, KEY_READ)
        .is_ok()
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// ENUMERATION FUNCTIONS
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Enumerate all subkeys (child keys) of a registry key.
///
/// # What This Does
///
/// Lists all direct children of a registry key. For example, under
/// `HKEY_CURRENT_USER\Software`, this would return keys like
/// "Microsoft", "Google", "Mozilla", etc.
///
/// # Why Sorted?
///
/// Results are sorted case-insensitively for consistent display in the
/// tree view. Windows doesn't guarantee any order when enumerating.
///
/// # Error Handling
///
/// Individual subkey enumeration errors are silently skipped. This handles
/// cases where we have partial access to a key's children.
///
/// # Arguments
/// * `root` - The root key to search under
/// * `path` - Path to the parent key (empty = root key itself)
///
/// # Returns
/// * `Ok(Vec<String>)` - List of subkey names (not full paths)
/// * `Err(String)` - If we can't open the parent key
///
/// # Example
///
/// ```rust
/// // List all software installed for current user
/// let vendors = enumerate_subkeys(
///     &RootKey::HkeyCurrentUser,
///     "Software"
/// )?;
/// // vendors might be: ["Google", "Microsoft", "Mozilla", ...]
/// ```
pub fn enumerate_subkeys(root: &RootKey, path: &str) -> Result<Vec<String>, String> {
    // Get the root key handle
    let reg_key = root.reg_key();
    
    // Open the parent key
    let key = if path.is_empty() {
        reg_key
    } else {
        reg_key
            .open_subkey_with_flags(path, KEY_READ)
            .map_err(|e| format!("Failed to open key: {}", e))?
    };

    // Collect all subkey names
    let mut subkeys = Vec::new();
    for name in key.enum_keys() {
        match name {
            Ok(n) => subkeys.push(n),
            Err(_) => continue, // Skip keys we can't read
        }
    }
    
    // Sort case-insensitively for consistent UI display
    subkeys.sort_by(|a, b| a.to_lowercase().cmp(&b.to_lowercase()));
    Ok(subkeys)
}

/// Enumerate all values in a registry key.
///
/// # What This Does
///
/// Lists all named values stored in a registry key. Each value has:
/// - A name (or empty string for the "Default" value)
/// - A type (REG_SZ, REG_DWORD, etc.)
/// - The actual data
///
/// # The Default Value
///
/// Every key has a "default" value with an empty name (`""`).
/// In regedit.exe, this shows as "(Default)".
///
/// # Arguments
/// * `root` - The root key
/// * `path` - Path to the key containing the values
///
/// # Returns
/// * `Ok(Vec<RegistryValue>)` - All values, sorted by name
/// * `Err(String)` - If we can't open the key
///
/// # Example
///
/// ```rust
/// // Read all environment variables
/// let values = enumerate_values(
///     &RootKey::HkeyCurrentUser,
///     "Environment"
/// )?;
/// for val in values {
///     println!("{} = {}", val.name, val.data);
/// }
/// ```
pub fn enumerate_values(root: &RootKey, path: &str) -> Result<Vec<RegistryValue>, String> {
    let reg_key = root.reg_key();
    
    let key = if path.is_empty() {
        reg_key
    } else {
        reg_key
            .open_subkey_with_flags(path, KEY_READ)
            .map_err(|e| format!("Failed to open key: {}", e))?
    };

    let mut values = Vec::new();
    for result in key.enum_values() {
        match result {
            Ok((name, val)) => {
                // Convert from winreg's type to our type
                values.push(RegistryValue {
                    name,
                    data: from_winreg_value(&val),
                });
            }
            Err(_) => continue, // Skip values we can't read
        }
    }
    
    // Sort by name for consistent display
    values.sort_by(|a, b| a.name.to_lowercase().cmp(&b.name.to_lowercase()));
    Ok(values)
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// KEY MODIFICATION FUNCTIONS
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Create a new subkey under an existing registry key.
///
/// # What This Does
///
/// Creates a new empty registry key as a child of an existing key.
/// If the key already exists, this is a no-op (doesn't error).
///
/// # Permissions Required
///
/// Requires KEY_WRITE | KEY_READ on the parent key.
/// May fail for protected system keys.
///
/// # Arguments
/// * `root` - The root key (HKEY_CURRENT_USER, etc.)
/// * `path` - Path to the parent key (where the new key will be created)
/// * `name` - Name of the new key to create
///
/// # Returns
/// * `Ok(())` - Key was created (or already existed)
/// * `Err(String)` - Permission denied or parent doesn't exist
///
/// # Example
///
/// ```rust
/// // Create HKEY_CURRENT_USER\Software\MyApp\Settings
/// create_key(
///     &RootKey::HkeyCurrentUser,
///     "Software\\MyApp",    // parent
///     "Settings"            // new key name
/// )?;
/// ```
pub fn create_key(root: &RootKey, path: &str, name: &str) -> Result<(), String> {
    let reg_key = root.reg_key();
    
    // Open the parent key with write access
    let parent = if path.is_empty() {
        reg_key
    } else {
        reg_key
            .open_subkey_with_flags(path, KEY_WRITE | KEY_READ)
            .map_err(|e| format!("Failed to open parent key: {}", e))?
    };

    // Create the new subkey
    // Note: create_subkey returns (key_handle, disposition) but we ignore both
    let full_name = name;
    parent
        .create_subkey(full_name)
        .map_err(|e| format!("Failed to create key: {}", e))?;
    Ok(())
}

/// Delete a registry key and all its contents recursively.
///
/// # ⚠️ WARNING: DESTRUCTIVE OPERATION
///
/// This deletes the key AND all subkeys/values underneath it.
/// There is no undo! The data is permanently gone.
///
/// # What This Uses
///
/// Uses `delete_subkey_all` which recursively deletes:
/// - The named key
/// - All subkeys under it
/// - All values in all those keys
///
/// # Arguments
/// * `root` - The root key
/// * `path` - Path to the parent key
/// * `name` - Name of the subkey to delete
///
/// # Returns
/// * `Ok(())` - Key was deleted
/// * `Err(String)` - Permission denied, key in use, or doesn't exist
///
/// # Example
///
/// ```rust
/// // Delete HKEY_CURRENT_USER\Software\MyApp (and everything under it)
/// delete_key(
///     &RootKey::HkeyCurrentUser,
///     "Software",
///     "MyApp"
/// )?;
/// ```
pub fn delete_key(root: &RootKey, path: &str, name: &str) -> Result<(), String> {
    let reg_key = root.reg_key();
    
    let parent = if path.is_empty() {
        reg_key
    } else {
        reg_key
            .open_subkey_with_flags(path, KEY_WRITE | KEY_READ)
            .map_err(|e| format!("Failed to open parent key: {}", e))?
    };

    // delete_subkey_all is recursive - it removes all children too
    parent
        .delete_subkey_all(name)
        .map_err(|e| format!("Failed to delete key: {}", e))?;
    Ok(())
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// VALUE MODIFICATION FUNCTIONS
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Set (create or update) a registry value.
///
/// # What This Does
///
/// Creates a new value or updates an existing one. The value type
/// is determined by the `RegValue` variant:
///
/// | RegValue Variant | Registry Type |
/// |------------------|---------------|
/// | String | REG_SZ |
/// | ExpandString | REG_EXPAND_SZ |
/// | MultiString | REG_MULTI_SZ |
/// | Dword | REG_DWORD |
/// | Qword | REG_QWORD |
/// | Binary | REG_BINARY |
///
/// # Special Handling
///
/// - **String/Dword/Qword**: Use winreg's native `set_value`
/// - **ExpandString/MultiString/Binary**: Require manual encoding via `set_raw_value`
/// - **None/Unknown**: Not supported for writing
///
/// # Arguments
/// * `root` - The root key
/// * `path` - Path to the key containing the value
/// * `name` - Value name (empty string = default value)
/// * `value` - The typed data to write
///
/// # Returns
/// * `Ok(())` - Value was written
/// * `Err(String)` - Permission denied, invalid type, or key doesn't exist
///
/// # Example
///
/// ```rust
/// // Set a string value
/// set_value(
///     &RootKey::HkeyCurrentUser,
///     "Environment",
///     "MY_VAR",
///     &RegValue::String("my value".to_string())
/// )?;
///
/// // Set a DWORD value
/// set_value(
///     &RootKey::HkeyCurrentUser,
///     "Software\\MyApp",
///     "Enabled",
///     &RegValue::Dword(1)
/// )?;
/// ```
pub fn set_value(
    root: &RootKey,
    path: &str,
    name: &str,
    value: &RegValue,
) -> Result<(), String> {
    let reg_key = root.reg_key();
    
    // Open with KEY_SET_VALUE permission
    let key = if path.is_empty() {
        reg_key
    } else {
        reg_key
            .open_subkey_with_flags(path, KEY_SET_VALUE)
            .map_err(|e| format!("Failed to open key: {}", e))?
    };

    // Write the value based on its type
    match value {
        // Simple types use winreg's native set_value
        RegValue::String(s) => key
            .set_value(name, s)
            .map_err(|e| format!("Failed to set value: {}", e)),
            
        RegValue::Dword(d) => key
            .set_value(name, d)
            .map_err(|e| format!("Failed to set value: {}", e)),
            
        RegValue::Qword(q) => key
            .set_value(name, q)
            .map_err(|e| format!("Failed to set value: {}", e)),
            
        // ExpandString needs manual UTF-16 encoding
        RegValue::ExpandString(s) => {
            let winreg_val = winreg::RegValue {
                // Encode as UTF-16LE with null terminator
                bytes: s
                    .encode_utf16()
                    .chain(std::iter::once(0))  // Add null terminator
                    .flat_map(|c| c.to_le_bytes())
                    .collect(),
                vtype: REG_EXPAND_SZ,
            };
            key.set_raw_value(name, &winreg_val)
                .map_err(|e| format!("Failed to set value: {}", e))
        }
        
        // MultiString is double-null terminated
        RegValue::MultiString(strings) => {
            let winreg_val = winreg::RegValue {
                // Each string null-terminated, then extra null at end
                bytes: strings
                    .iter()
                    .flat_map(|s| s.encode_utf16().chain(std::iter::once(0)))
                    .chain(std::iter::once(0))  // Final null terminator
                    .flat_map(|c| c.to_le_bytes())
                    .collect(),
                vtype: REG_MULTI_SZ,
            };
            key.set_raw_value(name, &winreg_val)
                .map_err(|e| format!("Failed to set value: {}", e))
        }
        
        // Binary is just raw bytes
        RegValue::Binary(b) => {
            let winreg_val = winreg::RegValue {
                bytes: b.clone(),
                vtype: REG_BINARY,
            };
            key.set_raw_value(name, &winreg_val)
                .map_err(|e| format!("Failed to set value: {}", e))
        }
        
        // None and Unknown types can't be edited
        _ => Err("Unsupported value type for editing".to_string()),
    }
}

/// Delete a registry value from a key.
///
/// # What This Does
///
/// Removes a named value from a registry key. The key itself
/// remains; only the value is deleted.
///
/// # Note on Default Value
///
/// The "default" value (empty name) can be deleted, but the
/// slot still exists - it just becomes "(value not set)".
///
/// # Arguments
/// * `root` - The root key
/// * `path` - Path to the key containing the value
/// * `name` - Name of the value to delete
///
/// # Returns
/// * `Ok(())` - Value was deleted
/// * `Err(String)` - Permission denied or value doesn't exist
pub fn delete_value(root: &RootKey, path: &str, name: &str) -> Result<(), String> {
    let reg_key = root.reg_key();
    
    let key = if path.is_empty() {
        reg_key
    } else {
        reg_key
            .open_subkey_with_flags(path, KEY_SET_VALUE)
            .map_err(|e| format!("Failed to open key: {}", e))?
    };

    key.delete_value(name)
        .map_err(|e| format!("Failed to delete value: {}", e))
}

/// Rename a registry value (implemented as copy + delete).
///
/// # Why Copy + Delete?
///
/// The Windows Registry API has no "rename value" operation.
/// We must:
/// 1. Read the old value's data
/// 2. Write it with the new name
/// 3. Delete the old value
///
/// # Atomicity
///
/// This is NOT atomic! If the process crashes between steps 2 and 3,
/// you'll have duplicate values. In practice, this is rare.
///
/// # Arguments
/// * `root` - The root key
/// * `path` - Path to the key containing the value
/// * `old_name` - Current name of the value
/// * `new_name` - New name for the value
///
/// # Returns
/// * `Ok(())` - Value was renamed
/// * `Err(String)` - Permission denied or value doesn't exist
pub fn rename_value(
    root: &RootKey,
    path: &str,
    old_name: &str,
    new_name: &str,
) -> Result<(), String> {
    let reg_key = root.reg_key();
    
    // Need both read (to get old value) and write (to set new, delete old)
    let key = if path.is_empty() {
        reg_key
    } else {
        reg_key
            .open_subkey_with_flags(path, KEY_READ | KEY_SET_VALUE)
            .map_err(|e| format!("Failed to open key: {}", e))?
    };

    // Step 1: Read the old value (preserving type and data)
    let val = key
        .get_raw_value(old_name)
        .map_err(|e| format!("Failed to read value: {}", e))?;
    
    // Step 2: Write with new name
    key.set_raw_value(new_name, &val)
        .map_err(|e| format!("Failed to set new value: {}", e))?;
    
    // Step 3: Delete old value
    key.delete_value(old_name)
        .map_err(|e| format!("Failed to delete old value: {}", e))?;
    
    Ok(())
}
