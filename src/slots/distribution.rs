//! Pure slot distribution calculations.
//!
//! All functions in this module are pure, side-effect free, and fully testable.
//! They compute how slots should be distributed across master nodes.

/// Total number of hash slots in a Valkey cluster.
pub const TOTAL_SLOTS: u16 = 16384;

/// A contiguous range of hash slots [start, end] inclusive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotRange {
    /// Start of the slot range (inclusive).
    pub start: u16,
    /// End of the slot range (inclusive).
    pub end: u16,
}

impl SlotRange {
    /// Create a new slot range.
    ///
    /// # Panics
    /// Debug-only assertions check that start <= end and end < TOTAL_SLOTS.
    pub fn new(start: u16, end: u16) -> Self {
        debug_assert!(start <= end, "start ({}) must be <= end ({})", start, end);
        debug_assert!(
            end < TOTAL_SLOTS,
            "end ({}) must be < TOTAL_SLOTS ({})",
            end,
            TOTAL_SLOTS
        );
        Self { start, end }
    }

    /// Create a single-slot range.
    pub fn single(slot: u16) -> Self {
        Self {
            start: slot,
            end: slot,
        }
    }

    /// Get the number of slots in this range.
    pub fn count(&self) -> u16 {
        self.end - self.start + 1
    }

    /// Check if this range contains a specific slot.
    pub fn contains(&self, slot: u16) -> bool {
        slot >= self.start && slot <= self.end
    }

    /// Iterate over all slots in this range.
    pub fn iter(&self) -> impl Iterator<Item = u16> {
        self.start..=self.end
    }
}

impl std::fmt::Display for SlotRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.start == self.end {
            write!(f, "{}", self.start)
        } else {
            write!(f, "{}-{}", self.start, self.end)
        }
    }
}

/// Calculate ideal slot distribution for N masters.
///
/// Returns Vec of `SlotRange` tuples.
/// Distributes remainder slots to earlier nodes (nodes 0..remainder get one extra slot).
///
/// # Examples
///
/// ```
/// use valkey_operator::slots::distribution::{calculate_distribution, SlotRange};
///
/// let dist = calculate_distribution(3);
/// assert_eq!(dist[0], SlotRange::new(0, 5461));      // 5462 slots
/// assert_eq!(dist[1], SlotRange::new(5462, 10922)); // 5461 slots
/// assert_eq!(dist[2], SlotRange::new(10923, 16383)); // 5461 slots
/// ```
pub fn calculate_distribution(master_count: u16) -> Vec<SlotRange> {
    if master_count == 0 {
        return Vec::new();
    }

    let slots_per_master = TOTAL_SLOTS / master_count;
    let remainder = TOTAL_SLOTS % master_count;

    let mut ranges = Vec::with_capacity(master_count as usize);
    let mut start: u16 = 0;

    for i in 0..master_count {
        let extra = if i < remainder { 1 } else { 0 };
        let count = slots_per_master + extra;
        let end = start + count - 1;

        ranges.push(SlotRange::new(start, end));
        start = end + 1;
    }

    ranges
}

/// Determine which master index owns a given slot.
///
/// This is the inverse of `calculate_distribution` - given a slot, return the master index.
///
/// # Examples
///
/// ```
/// use valkey_operator::slots::distribution::slot_owner;
///
/// // With 3 masters: slots 0-5461 go to master 0
/// assert_eq!(slot_owner(0, 3), 0);
/// assert_eq!(slot_owner(5461, 3), 0);
/// assert_eq!(slot_owner(5462, 3), 1);
/// assert_eq!(slot_owner(16383, 3), 2);
/// ```
pub fn slot_owner(slot: u16, master_count: u16) -> u16 {
    if master_count == 0 {
        return 0;
    }

    let slots_per_master = TOTAL_SLOTS / master_count;
    let remainder = TOTAL_SLOTS % master_count;
    let boundary = remainder * (slots_per_master + 1);

    if slot < boundary {
        slot / (slots_per_master + 1)
    } else {
        remainder + (slot - boundary) / slots_per_master
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::get_unwrap
)]
mod tests {
    use super::*;

    #[test]
    fn test_slot_range_new() {
        let range = SlotRange::new(0, 5461);
        assert_eq!(range.start, 0);
        assert_eq!(range.end, 5461);
    }

    #[test]
    fn test_slot_range_single() {
        let range = SlotRange::single(100);
        assert_eq!(range.start, 100);
        assert_eq!(range.end, 100);
        assert_eq!(range.count(), 1);
    }

    #[test]
    fn test_slot_range_count() {
        assert_eq!(SlotRange::new(0, 5461).count(), 5462);
        assert_eq!(SlotRange::single(100).count(), 1);
        assert_eq!(SlotRange::new(0, 16383).count(), 16384);
    }

    #[test]
    fn test_slot_range_contains() {
        let range = SlotRange::new(100, 200);
        assert!(range.contains(100));
        assert!(range.contains(150));
        assert!(range.contains(200));
        assert!(!range.contains(99));
        assert!(!range.contains(201));
    }

    #[test]
    fn test_slot_range_iter() {
        let range = SlotRange::new(0, 2);
        let slots: Vec<u16> = range.iter().collect();
        assert_eq!(slots, vec![0, 1, 2]);
    }

    #[test]
    fn test_slot_range_display() {
        assert_eq!(format!("{}", SlotRange::new(0, 5461)), "0-5461");
        assert_eq!(format!("{}", SlotRange::single(100)), "100");
    }

    #[test]
    fn test_calculate_distribution_zero_masters() {
        let dist = calculate_distribution(0);
        assert!(dist.is_empty());
    }

    #[test]
    fn test_calculate_distribution_one_master() {
        let dist = calculate_distribution(1);
        assert_eq!(dist.len(), 1);
        assert_eq!(dist.first().unwrap(), &SlotRange::new(0, 16383));
    }

    #[test]
    fn test_calculate_distribution_three_masters() {
        let dist = calculate_distribution(3);
        assert_eq!(dist.len(), 3);
        // 16384 / 3 = 5461 remainder 1
        // First master gets 5462 slots: 0-5461
        // Second master gets 5461 slots: 5462-10922
        // Third master gets 5461 slots: 10923-16383
        assert_eq!(dist.first().unwrap(), &SlotRange::new(0, 5461));
        assert_eq!(dist.get(1).unwrap(), &SlotRange::new(5462, 10922));
        assert_eq!(dist.get(2).unwrap(), &SlotRange::new(10923, 16383));
    }

    #[test]
    fn test_calculate_distribution_six_masters() {
        let dist = calculate_distribution(6);
        assert_eq!(dist.len(), 6);

        // Verify total coverage
        let total: u16 = dist.iter().map(|r| r.count()).sum();
        assert_eq!(total, TOTAL_SLOTS);

        // Verify ranges are contiguous
        assert_eq!(dist.first().unwrap().start, 0);
        assert_eq!(dist.get(5).unwrap().end, 16383);
        for i in 1..dist.len() {
            assert_eq!(dist.get(i).unwrap().start, dist.get(i - 1).unwrap().end + 1);
        }
    }

    #[test]
    fn test_distribution_covers_all_slots() {
        for master_count in 1..=20 {
            let dist = calculate_distribution(master_count);
            let total: u16 = dist.iter().map(|r| r.count()).sum();
            assert_eq!(
                total, TOTAL_SLOTS,
                "master_count={} should cover all slots",
                master_count
            );
        }
    }

    #[test]
    fn test_distribution_is_contiguous() {
        for master_count in 1..=20 {
            let dist = calculate_distribution(master_count);
            assert_eq!(dist.first().unwrap().start, 0);
            assert_eq!(dist.last().map(|r| r.end), Some(TOTAL_SLOTS - 1));
            for i in 1..dist.len() {
                assert_eq!(
                    dist.get(i).unwrap().start,
                    dist.get(i - 1).unwrap().end + 1,
                    "Ranges must be contiguous for master_count={}",
                    master_count
                );
            }
        }
    }

    #[test]
    fn test_slot_owner_zero_masters() {
        assert_eq!(slot_owner(0, 0), 0);
        assert_eq!(slot_owner(100, 0), 0);
    }

    #[test]
    fn test_slot_owner_one_master() {
        assert_eq!(slot_owner(0, 1), 0);
        assert_eq!(slot_owner(16383, 1), 0);
    }

    #[test]
    fn test_slot_owner_three_masters() {
        assert_eq!(slot_owner(0, 3), 0);
        assert_eq!(slot_owner(5461, 3), 0);
        assert_eq!(slot_owner(5462, 3), 1);
        assert_eq!(slot_owner(10922, 3), 1);
        assert_eq!(slot_owner(10923, 3), 2);
        assert_eq!(slot_owner(16383, 3), 2);
    }

    #[test]
    fn test_slot_owner_is_inverse_of_distribution() {
        for master_count in 1..=10 {
            let dist = calculate_distribution(master_count);
            for (idx, range) in dist.iter().enumerate() {
                for slot in range.iter() {
                    assert_eq!(
                        slot_owner(slot, master_count),
                        idx as u16,
                        "slot_owner should be inverse of distribution for slot {} with {} masters",
                        slot,
                        master_count
                    );
                }
            }
        }
    }
}
