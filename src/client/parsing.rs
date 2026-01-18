//! Standardized parsing module for Valkey text output.
//!
//! This module provides robust, testable parsing functions using regex
//! to replace fragile string splitting throughout the codebase.
//!
//! All parsing functions are:
//! - Standardized (use regex patterns)
//! - Reproducible (same input always produces same output)
//! - Easily testable (pure functions with clear inputs/outputs)
//! - Robust (handle edge cases gracefully)

use regex::Regex;
use thiserror::Error;

/// Errors that can occur during parsing.
#[derive(Error, Debug)]
pub enum ParseError {
    #[error("Failed to compile regex: {0}")]
    RegexCompilation(String),

    #[error("Parse error: {0}")]
    Parse(String),

    #[error("Missing required field: {0}")]
    MissingField(String),
}

/// Parse key-value pairs from INFO command output.
///
/// INFO output format: `key:value` per line, with optional section headers starting with `#`.
///
/// # Example
/// ```
/// use valkey_operator::client::parsing::parse_info_output;
///
/// let info = "master_repl_offset:12345\nslave_repl_offset:12340\n";
/// let parsed = parse_info_output(info).unwrap();
/// assert_eq!(parsed.get("master_repl_offset"), Some(&"12345".to_string()));
/// ```
pub fn parse_info_output(
    info: &str,
) -> Result<std::collections::HashMap<String, String>, ParseError> {
    // Regex for key:value pairs
    // Matches: key:value (where key is word chars, value is anything after colon)
    let kv_regex =
        Regex::new(r"^([\w-]+):(.+)$").map_err(|e| ParseError::RegexCompilation(e.to_string()))?;

    let mut result = std::collections::HashMap::new();

    for line in info.lines() {
        let line = line.trim();

        // Skip empty lines and section headers
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        if let Some(caps) = kv_regex.captures(line)
            && let (Some(key), Some(value)) = (caps.get(1), caps.get(2))
        {
            result.insert(key.as_str().to_string(), value.as_str().to_string());
        }
    }

    Ok(result)
}

/// Parse a specific value from INFO output by key name.
///
/// Returns the value as a string, or None if not found.
pub fn parse_info_value(info: &str, key: &str) -> Option<String> {
    parse_info_output(info)
        .ok()
        .and_then(|map| map.get(key).cloned())
}

/// Parse an integer value from INFO output.
///
/// Returns the parsed integer, or None if not found or invalid.
pub fn parse_info_int(info: &str, key: &str) -> Option<i64> {
    parse_info_value(info, key).and_then(|v| v.trim().parse().ok())
}

/// Parse replication information from INFO REPLICATION output.
///
/// Returns a structured representation of replication status.
#[derive(Debug, Clone, Default)]
pub struct ReplicationInfo {
    /// Master link status ("up" or "down")
    pub master_link_status: Option<String>,
    /// Master's replication offset
    pub master_repl_offset: Option<i64>,
    /// Replica's replication offset
    pub slave_repl_offset: Option<i64>,
    /// Replica's read replication offset
    pub slave_read_repl_offset: Option<i64>,
    /// Role ("master" or "slave")
    pub role: Option<String>,
}

impl ReplicationInfo {
    /// Parse from INFO REPLICATION output string.
    pub fn parse(info: &str) -> Result<Self, ParseError> {
        let parsed = parse_info_output(info)?;

        Ok(ReplicationInfo {
            master_link_status: parsed.get("master_link_status").cloned(),
            master_repl_offset: parsed
                .get("master_repl_offset")
                .and_then(|v| v.trim().parse().ok()),
            slave_repl_offset: parsed
                .get("slave_repl_offset")
                .and_then(|v| v.trim().parse().ok()),
            slave_read_repl_offset: parsed
                .get("slave_read_repl_offset")
                .and_then(|v| v.trim().parse().ok()),
            role: parsed.get("role").cloned(),
        })
    }

    /// Check if replication is in sync by comparing offsets.
    ///
    /// Requires both master_repl_offset and slave_repl_offset to be available.
    pub fn is_in_sync(&self) -> Option<bool> {
        match (self.master_repl_offset, self.slave_repl_offset) {
            (Some(master), Some(slave)) => Some(master == slave),
            _ => None,
        }
    }

    /// Get replication lag in bytes.
    ///
    /// Returns None if offsets are unavailable.
    pub fn replication_lag(&self) -> Option<i64> {
        match (self.master_repl_offset, self.slave_repl_offset) {
            (Some(master), Some(slave)) => Some(master - slave),
            _ => None,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_info_output_basic() {
        let info = "master_repl_offset:12345\nslave_repl_offset:12340\n";
        let parsed = parse_info_output(info).unwrap();
        assert_eq!(parsed.get("master_repl_offset"), Some(&"12345".to_string()));
        assert_eq!(parsed.get("slave_repl_offset"), Some(&"12340".to_string()));
    }

    #[test]
    fn test_parse_info_output_with_sections() {
        let info = "# Replication\nrole:master\nmaster_repl_offset:12345\n";
        let parsed = parse_info_output(info).unwrap();
        assert_eq!(parsed.get("role"), Some(&"master".to_string()));
        assert_eq!(parsed.get("master_repl_offset"), Some(&"12345".to_string()));
        // Section header should be skipped
        assert!(!parsed.contains_key("Replication"));
    }

    #[test]
    fn test_parse_info_value() {
        let info = "master_repl_offset:12345\n";
        assert_eq!(
            parse_info_value(info, "master_repl_offset"),
            Some("12345".to_string())
        );
        assert_eq!(parse_info_value(info, "nonexistent"), None);
    }

    #[test]
    fn test_parse_info_int() {
        let info = "master_repl_offset:12345\n";
        assert_eq!(parse_info_int(info, "master_repl_offset"), Some(12345));
        assert_eq!(parse_info_int(info, "nonexistent"), None);
    }

    #[test]
    fn test_replication_info_parse() {
        let info = "role:master\nmaster_repl_offset:12345\nslave_repl_offset:12340\nmaster_link_status:up\n";
        let repl_info = ReplicationInfo::parse(info).unwrap();
        assert_eq!(repl_info.role, Some("master".to_string()));
        assert_eq!(repl_info.master_repl_offset, Some(12345));
        assert_eq!(repl_info.slave_repl_offset, Some(12340));
        assert_eq!(repl_info.master_link_status, Some("up".to_string()));
    }

    #[test]
    fn test_replication_info_is_in_sync() {
        let info = "master_repl_offset:12345\nslave_repl_offset:12345\n";
        let repl_info = ReplicationInfo::parse(info).unwrap();
        assert_eq!(repl_info.is_in_sync(), Some(true));

        let info2 = "master_repl_offset:12345\nslave_repl_offset:12340\n";
        let repl_info2 = ReplicationInfo::parse(info2).unwrap();
        assert_eq!(repl_info2.is_in_sync(), Some(false));
    }

    #[test]
    fn test_replication_info_lag() {
        let info = "master_repl_offset:12345\nslave_repl_offset:12340\n";
        let repl_info = ReplicationInfo::parse(info).unwrap();
        assert_eq!(repl_info.replication_lag(), Some(5));
    }
}
