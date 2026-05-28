//! Domain newtypes that wrap primitive identifiers to prevent argument
//! swaps and stringly-typed bugs at the type level.
//!
//! These wrap the existing primitive types with `#[serde(transparent)]` so
//! the JSON/CRD wire format is unchanged. CRD spec fields stay as primitives
//! (`i32` for `masters`, etc.) — these newtypes are for the operator's
//! internal call sites where confusing two `String`s or two `i32`s is easy.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A Valkey cluster node identifier (40 hex characters in production, but
/// not type-enforced — Valkey itself emits the format and we trust it).
///
/// Distinguishes `node_id` from other `String` values like pod names,
/// namespaces, and address strings at every function boundary.
#[derive(
    Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Deserialize, Serialize, JsonSchema,
)]
#[serde(transparent)]
pub struct NodeId(pub String);

impl NodeId {
    /// Construct from any owned or borrowed string.
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    /// Borrowed view of the underlying string.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consume the newtype and return the inner `String`.
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for NodeId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<String> for NodeId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for NodeId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl std::borrow::Borrow<str> for NodeId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

/// A pod ordinal within a StatefulSet (zero-indexed).
///
/// Wraps `u16` so call sites cannot mix pod ordinals with other small
/// integers (slot IDs, replica counts, ports). The underlying type is `u16`
/// because Valkey clusters fit comfortably below 65535 pods.
#[derive(
    Copy,
    Clone,
    Debug,
    Default,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
    Deserialize,
    Serialize,
    JsonSchema,
)]
#[serde(transparent)]
pub struct PodOrdinal(pub u16);

impl PodOrdinal {
    /// Construct from a `u16`.
    pub fn new(n: u16) -> Self {
        Self(n)
    }

    /// The inner `u16`.
    pub fn get(self) -> u16 {
        self.0
    }
}

impl std::fmt::Display for PodOrdinal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u16> for PodOrdinal {
    fn from(n: u16) -> Self {
        Self(n)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn node_id_serializes_transparently() {
        let id = NodeId::new("abc123");
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "\"abc123\"");
    }

    #[test]
    fn node_id_deserializes_from_bare_string() {
        let id: NodeId = serde_json::from_str("\"abc123\"").unwrap();
        assert_eq!(id.as_str(), "abc123");
    }

    #[test]
    fn pod_ordinal_serializes_transparently() {
        let ord = PodOrdinal::new(7);
        let json = serde_json::to_string(&ord).unwrap();
        assert_eq!(json, "7");
    }

    #[test]
    fn pod_ordinal_round_trips() {
        let ord: PodOrdinal = serde_json::from_str("12").unwrap();
        assert_eq!(ord.get(), 12);
    }

    #[test]
    fn node_id_hash_eq_works_for_map_lookup() {
        let mut map = std::collections::HashMap::new();
        map.insert(NodeId::new("a"), 1);
        assert_eq!(map.get(&NodeId::new("a")), Some(&1));
    }

    #[test]
    fn node_id_borrow_str_allows_str_lookup() {
        let mut map = std::collections::HashMap::new();
        map.insert(NodeId::new("a"), 1);
        // Borrow<str> impl means we can look up by &str directly.
        assert_eq!(map.get("a"), Some(&1));
    }
}
