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

    /// True when the underlying string is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl PartialEq<str> for NodeId {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<&str> for NodeId {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

impl PartialEq<String> for NodeId {
    fn eq(&self, other: &String) -> bool {
        &self.0 == other
    }
}

impl PartialEq<NodeId> for String {
    fn eq(&self, other: &NodeId) -> bool {
        self == &other.0
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
/// Wraps `i32` to match the existing codebase convention (Kubernetes API
/// types use `i32` for replica counts and StatefulSet positions). Distinct
/// from other `i32` values like ports, counts, and indices at function
/// boundaries.
///
/// Negative values are not semantically valid (an ordinal is always ≥ 0) but
/// the type does not enforce this — validation happens at the boundary with
/// Kubernetes (`extract_ordinal_from_address`, `get_ordinal_from_address`)
/// which already returns `Option<_>`.
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
pub struct PodOrdinal(pub i32);

impl PodOrdinal {
    /// Construct from an `i32`.
    pub fn new(n: i32) -> Self {
        Self(n)
    }

    /// The inner `i32`.
    pub fn get(self) -> i32 {
        self.0
    }
}

impl std::fmt::Display for PodOrdinal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<i32> for PodOrdinal {
    fn from(n: i32) -> Self {
        Self(n)
    }
}

impl PartialEq<i32> for PodOrdinal {
    fn eq(&self, other: &i32) -> bool {
        self.0 == *other
    }
}

impl PartialOrd<i32> for PodOrdinal {
    fn partial_cmp(&self, other: &i32) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(other)
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
    fn pod_ordinal_arithmetic_via_get() {
        let a = PodOrdinal::new(3);
        let b = PodOrdinal::new(5);
        // Operations on inner i32 still require explicit .get() — protects
        // against accidental mixing with other i32 values.
        assert_eq!(a.get() + b.get(), 8);
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
