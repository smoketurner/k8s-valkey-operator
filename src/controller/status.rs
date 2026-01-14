//! Status management utilities.
//!
//! Provides helpers for building and updating resource status conditions.

use crate::crd::Condition;

/// Builder for managing conditions list
pub struct ConditionBuilder {
    conditions: Vec<Condition>,
}

impl ConditionBuilder {
    /// Create a new condition builder
    pub fn new() -> Self {
        Self {
            conditions: Vec::new(),
        }
    }

    /// Add or update a condition
    pub fn set(&mut self, condition: Condition) -> &mut Self {
        // Find and replace existing condition of same type
        if let Some(existing) = self
            .conditions
            .iter_mut()
            .find(|c| c.r#type == condition.r#type)
        {
            *existing = condition;
        } else {
            self.conditions.push(condition);
        }
        self
    }

    /// Set Ready condition
    pub fn ready(
        &mut self,
        ready: bool,
        reason: &str,
        message: &str,
        generation: Option<i64>,
    ) -> &mut Self {
        self.set(Condition::ready(ready, reason, message, generation))
    }

    /// Set Progressing condition
    pub fn progressing(
        &mut self,
        progressing: bool,
        reason: &str,
        message: &str,
        generation: Option<i64>,
    ) -> &mut Self {
        self.set(Condition::progressing(
            progressing,
            reason,
            message,
            generation,
        ))
    }

    /// Set Degraded condition
    pub fn degraded(
        &mut self,
        degraded: bool,
        reason: &str,
        message: &str,
        generation: Option<i64>,
    ) -> &mut Self {
        self.set(Condition::degraded(degraded, reason, message, generation))
    }

    /// Build the conditions list
    pub fn build(self) -> Vec<Condition> {
        self.conditions
    }
}

impl Default for ConditionBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Check if a condition type is true
pub fn is_condition_true(conditions: &[Condition], condition_type: &str) -> bool {
    conditions
        .iter()
        .find(|c| c.r#type == condition_type)
        .is_some_and(|c| c.status == "True")
}

/// Get the reason for a condition
pub fn get_condition_reason<'a>(
    conditions: &'a [Condition],
    condition_type: &str,
) -> Option<&'a str> {
    conditions
        .iter()
        .find(|c| c.r#type == condition_type)
        .map(|c| c.reason.as_str())
}
