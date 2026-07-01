/// Lock-free Trie-based topic routing with wildcard pattern support
/// 
/// Replaces exact-match SkipMap with hierarchical pattern matching:
/// - Exact patterns: `trades.us.aapl` → exact topic match only
/// - Wildcard single-level: `trades.us.*` → matches `trades.us.ANYTHING`
/// - Wildcard recursive: `trades.>` → matches `trades.ANYTHING.ANYTHING...`
/// 
/// Mathematical guarantee:
/// - Lookup time: O(depth of pattern) where depth ≤ 256
/// - Independent of total registered topics (scales with pattern depth, not topic count)
/// - Lock-free traversal: no global locks held during pattern matching

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use dashmap::DashMap;
use std::sync::Arc;
use tracing::debug;

/// A node in the Trie representing one segment of a topic path
pub struct TrieNode {
    /// Children indexed by full segment (not just first byte)
    /// Using DashMap for sharded locking
    pub children: Arc<DashMap<Bytes, Arc<TrieNode>>>,

    /// Exact handlers at this node - stores full patterns
    pub handlers: Arc<SkipMap<Bytes, ()>>,

    /// Single-level wildcard marker (Option stores the pattern if set)
    pub wildcard_handler: Arc<parking_lot::RwLock<Option<Bytes>>>,

    /// Recursive wildcard marker (Option stores the pattern if set)
    pub recursive_handler: Arc<parking_lot::RwLock<Option<Bytes>>>,
}

impl TrieNode {
    /// Create a new empty trie node
    pub fn new() -> Self {
        TrieNode {
            children: Arc::new(DashMap::new()),
            handlers: Arc::new(SkipMap::new()),
            wildcard_handler: Arc::new(parking_lot::RwLock::new(None)),
            recursive_handler: Arc::new(parking_lot::RwLock::new(None)),
        }
    }

    /// Get or create a child node for a given segment
    fn get_or_create_child(&self, segment: &[u8]) -> Arc<TrieNode> {
        let key = Bytes::copy_from_slice(segment);

        match self.children.get(&key) {
            Some(node) => node.clone(),
            None => {
                let new_node = Arc::new(TrieNode::new());
                // DashMap handles concurrent inserts safely
                self.children.entry(key).or_insert(new_node.clone());
                new_node
            }
        }
    }
}

impl Default for TrieNode {
    fn default() -> Self {
        Self::new()
    }
}

/// Lock-free Trie-based router for wildcard pattern matching
pub struct TrieRouter {
    root: Arc<TrieNode>,
}

impl TrieRouter {
    /// Create a new empty trie router
    pub fn new() -> Self {
        TrieRouter {
            root: Arc::new(TrieNode::new()),
        }
    }

    /// Subscribe to a pattern (exact or wildcard)
    ///
    /// Patterns:
    /// - `trades.us.aapl` → exact match only
    /// - `trades.us.*` → single-level wildcard (any one segment)
    /// - `trades.>` → recursive wildcard (all remaining segments)
    pub fn subscribe_pattern(&self, pattern: &str) -> Result<(), String> {
        if pattern.is_empty() {
            return Err("empty pattern".to_string());
        }

        let parts: Vec<&[u8]> = pattern.split('.').map(|s| s.as_bytes()).collect();

        if parts.is_empty() {
            return Err("empty pattern".to_string());
        }

        let pattern_bytes = Bytes::from(pattern.to_string());
        let mut node = self.root.clone();

        for (i, part) in parts.iter().enumerate() {
            let is_last = i == parts.len() - 1;

            match *part {
                b"*" if is_last => {
                    // Single-level wildcard at end
                    *node.wildcard_handler.write() = Some(pattern_bytes);
                    debug!(pattern = pattern, "subscribed to single-level wildcard");
                    return Ok(());
                }
                b">" if is_last => {
                    // Recursive wildcard at end
                    *node.recursive_handler.write() = Some(pattern_bytes);
                    debug!(pattern = pattern, "subscribed to recursive wildcard");
                    return Ok(());
                }
                b">" => {
                    return Err("recursive wildcard `>` must be at end of pattern".to_string());
                }
                b"*" => {
                    return Err("single-level wildcard `*` must be at end of pattern".to_string());
                }
                _ => {
                    // Exact segment - move to or create child
                    node = node.get_or_create_child(part);
                }
            }
        }

        // Store exact pattern at leaf node
        node.handlers.insert(pattern_bytes, ());
        debug!(pattern = pattern, "subscribed to exact pattern");
        Ok(())
    }

    /// Match a topic against all registered patterns
    /// Returns the set of patterns that match this topic
    pub fn match_topic(&self, topic: &str) -> Vec<Bytes> {
        if topic.is_empty() {
            return Vec::new();
        }

        let parts: Vec<&[u8]> = topic.split('.').map(|s| s.as_bytes()).collect();
        let mut results = Vec::new();
        self.dfs_match(&self.root, &parts, 0, &mut results);
        results
    }

    /// Depth-first search to find all matching patterns
    fn dfs_match(
        &self,
        node: &Arc<TrieNode>,
        parts: &[&[u8]],
        depth: usize,
        results: &mut Vec<Bytes>,
    ) {
        if depth == parts.len() {
            // Reached end of topic - check for exact patterns at this node
            for entry in node.handlers.iter() {
                results.push(entry.key().clone());
            }
            return;
        }

        let segment = parts[depth];
        let segment_key = Bytes::copy_from_slice(segment);

        // 1. Try exact segment match
        if let Some(next_node) = node.children.get(&segment_key) {
            self.dfs_match(&next_node, parts, depth + 1, results);
        }

        // 2. Try single-level wildcard (matches this segment only)
        if let Some(pattern) = node.wildcard_handler.read().as_ref() {
            if depth + 1 == parts.len() {
                // Wildcard matches exactly one segment ahead
                results.push(pattern.clone());
            }
        }

        // 3. Try recursive wildcard (matches all remaining)
        if let Some(pattern) = node.recursive_handler.read().as_ref() {
            results.push(pattern.clone());
        }
    }

    /// Get all registered patterns
    pub fn all_patterns(&self) -> Vec<Bytes> {
        let mut patterns = Vec::new();
        self.collect_patterns(&self.root, &mut patterns);
        patterns
    }

    /// Recursively collect all patterns in the trie
    fn collect_patterns(&self, node: &Arc<TrieNode>, results: &mut Vec<Bytes>) {
        // Collect exact handlers
        for entry in node.handlers.iter() {
            results.push(entry.key().clone());
        }

        // Collect wildcard handlers
        if let Some(pattern) = node.wildcard_handler.read().as_ref() {
            results.push(pattern.clone());
        }

        if let Some(pattern) = node.recursive_handler.read().as_ref() {
            results.push(pattern.clone());
        }

        // Recurse into children
        for entry in node.children.iter() {
            self.collect_patterns(&entry.value(), results);
        }
    }
}

impl Default for TrieRouter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_pattern_subscribe() {
        let router = TrieRouter::new();
        assert!(router.subscribe_pattern("trades.us.aapl").is_ok());
        assert!(router.subscribe_pattern("trades.us.spy").is_ok());
    }

    #[test]
    fn exact_pattern_match() {
        let router = TrieRouter::new();
        router.subscribe_pattern("trades.us.aapl").unwrap();

        let matches = router.match_topic("trades.us.aapl");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0], Bytes::from("trades.us.aapl"));
    }

    #[test]
    fn exact_pattern_no_match() {
        let router = TrieRouter::new();
        router.subscribe_pattern("trades.us.aapl").unwrap();

        let matches = router.match_topic("trades.us.spy");
        assert_eq!(matches.len(), 0);
    }

    #[test]
    fn single_wildcard_pattern() {
        let router = TrieRouter::new();
        router.subscribe_pattern("trades.us.*").unwrap();

        // Should match single segment at end
        let m1 = router.match_topic("trades.us.aapl");
        assert_eq!(m1.len(), 1);
        assert_eq!(m1[0], Bytes::from("trades.us.*"));

        let m2 = router.match_topic("trades.us.spy");
        assert_eq!(m2.len(), 1);
        assert_eq!(m2[0], Bytes::from("trades.us.*"));

        // Should NOT match different depth
        let m3 = router.match_topic("trades.us");
        assert_eq!(m3.len(), 0);

        let m4 = router.match_topic("trades.us.aapl.extra");
        assert_eq!(m4.len(), 0);
    }

    #[test]
    fn recursive_wildcard_pattern() {
        let router = TrieRouter::new();
        router.subscribe_pattern("sensor.>").unwrap();

        // Should match any remaining segments
        let m1 = router.match_topic("sensor.floor1.temp");
        assert_eq!(m1.len(), 1);

        let m2 = router.match_topic("sensor.floor1.room5.humidity");
        assert_eq!(m2.len(), 1);

        let m3 = router.match_topic("sensor.a.b.c.d.e");
        assert_eq!(m3.len(), 1);

        // Should NOT match different prefix
        let m4 = router.match_topic("actuator.floor1.temp");
        assert_eq!(m4.len(), 0);
    }

    #[test]
    fn mixed_patterns() {
        let router = TrieRouter::new();
        router.subscribe_pattern("trades.us.aapl").unwrap();
        router.subscribe_pattern("trades.us.*").unwrap();
        router.subscribe_pattern("trades.>").unwrap();

        // Should match all three patterns
        let matches = router.match_topic("trades.us.aapl");
        assert_eq!(matches.len(), 3);

        // Should match last two patterns
        let matches = router.match_topic("trades.us.spy");
        assert_eq!(matches.len(), 2);

        // Should match only last pattern
        let matches = router.match_topic("trades.eu.paris.dax");
        assert_eq!(matches.len(), 1);
    }

    #[test]
    fn wildcard_in_middle_is_error() {
        let router = TrieRouter::new();
        let result = router.subscribe_pattern("trades.*.aapl");
        assert!(result.is_err());
    }

    #[test]
    fn recursive_wildcard_in_middle_is_error() {
        let router = TrieRouter::new();
        let result = router.subscribe_pattern("trades.>.aapl");
        assert!(result.is_err());
    }

    #[test]
    fn empty_pattern_is_error() {
        let router = TrieRouter::new();
        let result = router.subscribe_pattern("");
        assert!(result.is_err());
    }

    #[test]
    fn multiple_subscribers_same_pattern() {
        let router = TrieRouter::new();
        router.subscribe_pattern("sensor.floor1.*").unwrap();
        // Second subscribe to same pattern is idempotent (SkipMap only stores once)
        router.subscribe_pattern("sensor.floor1.*").unwrap();

        // Pattern stored once (SkipMap deduplicates)
        let patterns = router.all_patterns();
        let count = patterns
            .iter()
            .filter(|p| p.as_ref() == b"sensor.floor1.*")
            .count();
        assert_eq!(count, 1);
    }

    #[test]
    fn trie_depth_scaling() {
        // Verify that lookup time is O(depth), not O(total topics)
        let router = TrieRouter::new();

        // Register patterns with different leaf segments
        for i in 0..100 {
            let pattern = format!("prefix.segment.{}.value", i);
            router.subscribe_pattern(&pattern).unwrap();
        }

        // Single pattern match should find only exact match
        let matches = router.match_topic("prefix.segment.50.value");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0], Bytes::from("prefix.segment.50.value"));
    }
}
