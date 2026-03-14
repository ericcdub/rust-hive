// Copyright (c) 2026 Eric Chubb
// Licensed under the MIT License

//! # Bookmarks Module
//!
//! This module provides bookmark functionality for saving frequently-accessed
//! registry locations. Bookmarks are stored in a JSON file in the user's
//! config directory.
//!
//! ## Features
//!
//! - Save and load bookmarks from JSON
//! - Color-coded bookmarks for organization
//! - Notes field for documentation
//! - Reorderable bookmark list
//!
//! ## Storage Location
//!
//! Bookmarks are stored at:
//! - Windows: `%APPDATA%\registry-editor\bookmarks.json`

use serde::{Deserialize, Serialize};

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// BOOKMARK - A saved registry location
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// A saved registry location with optional metadata.
///
/// # Fields
///
/// | Field | Description |
/// |-------|-------------|
/// | `name` | Display name for the bookmark |
/// | `path` | Full registry path (e.g., "HKEY_CURRENT_USER\\Software\\...") |
/// | `notes` | Optional notes/documentation |
/// | `color` | Optional color for visual organization |
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bookmark {
    /// Display name shown in the bookmarks list
    pub name: String,

    /// Full registry path including root key
    /// Example: "HKEY_CURRENT_USER\\Software\\Microsoft"
    pub path: String,

    /// Optional notes for documentation
    #[serde(default)]
    pub notes: String,

    /// Optional color for visual categorization
    #[serde(default)]
    pub color: Option<BookmarkColor>,
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// BOOKMARK COLOR - Visual categorization
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Available colors for bookmark categorization.
///
/// Colors are used to visually organize bookmarks in the UI.
/// Use `to_rgb()` to get the RGB values for rendering.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum BookmarkColor {
    Red,
    Green,
    Blue,
    Yellow,
    Purple,
    Orange,
}

impl BookmarkColor {
    /// Returns all available bookmark colors.
    pub fn all() -> &'static [BookmarkColor] {
        &[
            BookmarkColor::Red,
            BookmarkColor::Green,
            BookmarkColor::Blue,
            BookmarkColor::Yellow,
            BookmarkColor::Purple,
            BookmarkColor::Orange,
        ]
    }

    /// Converts the color to RGB values (0-255).
    ///
    /// # Returns
    /// A tuple of (red, green, blue) values.
    pub fn to_rgb(&self) -> (u8, u8, u8) {
        match self {
            BookmarkColor::Red => (220, 80, 80),
            BookmarkColor::Green => (80, 180, 80),
            BookmarkColor::Blue => (80, 130, 220),
            BookmarkColor::Yellow => (220, 200, 60),
            BookmarkColor::Purple => (160, 80, 200),
            BookmarkColor::Orange => (230, 150, 50),
        }
    }

    /// Returns the display name of this color.
    pub fn name(&self) -> &'static str {
        match self {
            BookmarkColor::Red => "Red",
            BookmarkColor::Green => "Green",
            BookmarkColor::Blue => "Blue",
            BookmarkColor::Yellow => "Yellow",
            BookmarkColor::Purple => "Purple",
            BookmarkColor::Orange => "Orange",
        }
    }

    /// Parse a color from its name string.
    pub fn from_name(name: &str) -> Option<BookmarkColor> {
        match name.to_lowercase().as_str() {
            "red" => Some(BookmarkColor::Red),
            "green" => Some(BookmarkColor::Green),
            "blue" => Some(BookmarkColor::Blue),
            "yellow" => Some(BookmarkColor::Yellow),
            "purple" => Some(BookmarkColor::Purple),
            "orange" => Some(BookmarkColor::Orange),
            _ => None,
        }
    }
}
