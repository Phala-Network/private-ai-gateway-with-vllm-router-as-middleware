//! Prefix-cache affinity index for router middleware.
//!
//! This is an in-process radix tree keyed by public model and route id. It is
//! intentionally owned by `RouterState`, which is already protected by a mutex,
//! so the tree does not need its own concurrent map or lock layer.

use std::collections::{HashMap, HashSet, VecDeque};

#[derive(Default)]
pub(super) struct CacheIndex {
    models: HashMap<String, ModelCache>,
    evictions_total: u64,
    removed_routes_total: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct CacheMatch {
    pub route_id: String,
    pub matched_chars: usize,
    pub input_chars: usize,
}

#[derive(Default, Clone, Copy)]
pub(super) struct CacheRouteStats {
    pub records: usize,
    pub chars: usize,
}

#[derive(Default, Clone, Copy)]
pub(super) struct CacheIndexStats {
    pub models: usize,
    pub routes: usize,
    pub records: usize,
    pub chars: usize,
    pub nodes: usize,
    pub evictions_total: u64,
    pub removed_routes_total: u64,
}

#[derive(Default)]
struct ModelCache {
    tree: PrefixTree,
    records: HashMap<String, VecDeque<String>>,
    route_chars: HashMap<String, usize>,
}

impl CacheIndex {
    pub(super) fn retain_model_routes(&mut self, model: &str, active_routes: &HashSet<String>) {
        let Some(cache) = self.models.get_mut(model) else {
            return;
        };
        let stale_routes = cache
            .records
            .keys()
            .filter(|route_id| !active_routes.contains(*route_id))
            .cloned()
            .collect::<Vec<_>>();
        for route_id in stale_routes {
            cache.remove_route(&route_id);
            self.removed_routes_total += 1;
        }
        if cache.records.is_empty() {
            self.models.remove(model);
        }
    }

    pub(super) fn match_prefix(&self, model: &str, text: &str) -> Option<CacheMatch> {
        if text.is_empty() {
            return None;
        }
        self.models.get(model)?.tree.prefix_match(text)
    }

    pub(super) fn record(
        &mut self,
        model: &str,
        route_id: &str,
        text: &str,
        max_records_per_route: usize,
    ) {
        if text.is_empty() || max_records_per_route == 0 {
            return;
        }
        let cache = self.models.entry(model.to_string()).or_default();
        cache.record(route_id, text);
        while cache
            .records
            .get(route_id)
            .is_some_and(|records| records.len() > max_records_per_route)
        {
            cache.evict_oldest(route_id);
            self.evictions_total += 1;
        }
    }

    pub(super) fn route_stats(&self, model: &str, route_id: &str) -> CacheRouteStats {
        self.models
            .get(model)
            .map(|cache| cache.route_stats(route_id))
            .unwrap_or_default()
    }

    pub(super) fn stats(&self) -> CacheIndexStats {
        let mut stats = CacheIndexStats {
            models: self.models.len(),
            evictions_total: self.evictions_total,
            removed_routes_total: self.removed_routes_total,
            ..Default::default()
        };
        for cache in self.models.values() {
            stats.routes += cache.records.len();
            stats.records += cache.records.values().map(VecDeque::len).sum::<usize>();
            stats.chars += cache.route_chars.values().sum::<usize>();
            stats.nodes += cache.tree.node_count();
        }
        stats
    }
}

impl ModelCache {
    fn record(&mut self, route_id: &str, text: &str) {
        let epoch = self.tree.next_epoch();
        self.tree.insert(text, route_id, epoch);
        self.route_chars
            .entry(route_id.to_string())
            .and_modify(|chars| *chars += text.chars().count())
            .or_insert_with(|| text.chars().count());
        self.records
            .entry(route_id.to_string())
            .or_default()
            .push_back(text.to_string());
    }

    fn evict_oldest(&mut self, route_id: &str) {
        let Some(records) = self.records.get_mut(route_id) else {
            return;
        };
        let Some(text) = records.pop_front() else {
            return;
        };
        let text_chars = text.chars().count();
        self.tree.remove(&text, route_id);
        if let Some(chars) = self.route_chars.get_mut(route_id) {
            *chars = chars.saturating_sub(text_chars);
            if *chars == 0 {
                self.route_chars.remove(route_id);
            }
        }
        if records.is_empty() {
            self.records.remove(route_id);
        }
    }

    fn remove_route(&mut self, route_id: &str) {
        let Some(records) = self.records.remove(route_id) else {
            return;
        };
        for text in records {
            self.tree.remove(&text, route_id);
        }
        self.route_chars.remove(route_id);
    }

    fn route_stats(&self, route_id: &str) -> CacheRouteStats {
        CacheRouteStats {
            records: self.records.get(route_id).map_or(0, VecDeque::len),
            chars: self.route_chars.get(route_id).copied().unwrap_or(0),
        }
    }
}

#[derive(Default)]
struct PrefixTree {
    root: Node,
    epoch: u64,
}

impl PrefixTree {
    fn next_epoch(&mut self) -> u64 {
        self.epoch = self.epoch.saturating_add(1);
        self.epoch
    }

    fn insert(&mut self, text: &str, route_id: &str, epoch: u64) {
        if text.is_empty() {
            return;
        }
        self.root.insert(text, route_id, epoch);
    }

    fn remove(&mut self, text: &str, route_id: &str) {
        if text.is_empty() {
            return;
        }
        self.root.remove(text, route_id);
    }

    fn prefix_match(&self, text: &str) -> Option<CacheMatch> {
        let input_chars = text.chars().count();
        if input_chars == 0 {
            return None;
        }

        let mut node = &self.root;
        let mut remaining = text;
        let mut matched_chars = 0usize;
        let mut matched_route = None;

        while !remaining.is_empty() {
            let Some(child) = node.child_with_prefix(remaining) else {
                break;
            };
            let common = shared_prefix(child.label.as_str(), remaining);
            if common.chars == 0 {
                break;
            }
            matched_chars += common.chars;
            matched_route = child.best_route().map(ToOwned::to_owned);
            if common.bytes < child.label.len() {
                break;
            }
            remaining = &remaining[common.bytes..];
            node = child;
        }

        matched_route.map(|route_id| CacheMatch {
            route_id,
            matched_chars,
            input_chars,
        })
    }

    fn node_count(&self) -> usize {
        self.root.node_count()
    }
}

#[derive(Default)]
struct Node {
    label: String,
    routes: HashMap<String, RouteRef>,
    best_route: Option<String>,
    children: Vec<Node>,
}

#[derive(Clone, Copy)]
struct RouteRef {
    count: usize,
    last_epoch: u64,
}

#[derive(Clone, Copy)]
struct SharedPrefix {
    bytes: usize,
    chars: usize,
}

impl Node {
    fn insert(&mut self, text: &str, route_id: &str, epoch: u64) {
        self.add_route_ref(route_id, epoch);

        let Some(first) = text.chars().next() else {
            return;
        };
        let Some(child_index) = self.children.iter().position(|child| {
            child
                .label
                .chars()
                .next()
                .is_some_and(|child_first| child_first == first)
        }) else {
            let mut child = Node {
                label: text.to_string(),
                ..Default::default()
            };
            child.add_route_ref(route_id, epoch);
            self.children.push(child);
            return;
        };

        let common = shared_prefix(self.children[child_index].label.as_str(), text);
        let child_label_len = self.children[child_index].label.len();
        if common.bytes == child_label_len {
            let remaining = &text[common.bytes..];
            self.children[child_index].insert(remaining, route_id, epoch);
            return;
        }

        let mut old_child = self.children.remove(child_index);
        let common_label = old_child.label[..common.bytes].to_string();
        old_child.label = old_child.label[common.bytes..].to_string();

        let mut split = Node {
            label: common_label,
            routes: old_child.routes.clone(),
            best_route: old_child.best_route.clone(),
            children: vec![old_child],
        };
        split.add_route_ref(route_id, epoch);

        let remaining = &text[common.bytes..];
        if !remaining.is_empty() {
            let mut new_child = Node {
                label: remaining.to_string(),
                ..Default::default()
            };
            new_child.add_route_ref(route_id, epoch);
            split.children.push(new_child);
        }
        self.children.push(split);
    }

    fn remove(&mut self, text: &str, route_id: &str) -> bool {
        self.remove_route_ref(route_id);
        if text.is_empty() {
            return self.routes.is_empty() && self.children.is_empty();
        }

        let Some(first) = text.chars().next() else {
            return self.routes.is_empty() && self.children.is_empty();
        };
        let Some(child_index) = self.children.iter().position(|child| {
            child
                .label
                .chars()
                .next()
                .is_some_and(|child_first| child_first == first)
        }) else {
            return self.routes.is_empty() && self.children.is_empty();
        };

        let child_label = self.children[child_index].label.as_str();
        if !text.starts_with(child_label) {
            return self.routes.is_empty() && self.children.is_empty();
        }
        let remaining = &text[child_label.len()..];
        if self.children[child_index].remove(remaining, route_id) {
            self.children.remove(child_index);
        }

        self.routes.is_empty() && self.children.is_empty()
    }

    fn add_route_ref(&mut self, route_id: &str, epoch: u64) {
        let route = self.routes.entry(route_id.to_string()).or_insert(RouteRef {
            count: 0,
            last_epoch: epoch,
        });
        route.count = route.count.saturating_add(1);
        route.last_epoch = route.last_epoch.max(epoch);
        self.refresh_best_route();
    }

    fn remove_route_ref(&mut self, route_id: &str) {
        let Some(route) = self.routes.get_mut(route_id) else {
            return;
        };
        route.count = route.count.saturating_sub(1);
        if route.count == 0 {
            self.routes.remove(route_id);
        }
        self.refresh_best_route();
    }

    fn refresh_best_route(&mut self) {
        self.best_route = self
            .routes
            .iter()
            .max_by(|(left_id, left), (right_id, right)| {
                left.last_epoch
                    .cmp(&right.last_epoch)
                    .then_with(|| right_id.cmp(left_id))
            })
            .map(|(route_id, _)| route_id.clone());
    }

    fn best_route(&self) -> Option<&str> {
        self.best_route.as_deref()
    }

    fn child_with_prefix(&self, text: &str) -> Option<&Node> {
        let first = text.chars().next()?;
        self.children.iter().find(|child| {
            child
                .label
                .chars()
                .next()
                .is_some_and(|child_first| child_first == first)
        })
    }

    fn node_count(&self) -> usize {
        1 + self.children.iter().map(Node::node_count).sum::<usize>()
    }
}

fn shared_prefix(left: &str, right: &str) -> SharedPrefix {
    let mut bytes = 0usize;
    let mut chars = 0usize;
    for ((left_idx, left_ch), (right_idx, right_ch)) in
        left.char_indices().zip(right.char_indices())
    {
        if left_ch != right_ch {
            break;
        }
        bytes = left_idx + left_ch.len_utf8();
        debug_assert_eq!(bytes, right_idx + right_ch.len_utf8());
        chars += 1;
    }
    SharedPrefix { bytes, chars }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn radix_tree_matches_shared_prefix_before_divergence() {
        let mut index = CacheIndex::default();
        index.record("m", "a:m", "shared prefix aaa", 16);

        let matched = index.match_prefix("m", "shared prefix bbb").unwrap();

        assert_eq!(matched.route_id, "a:m");
        assert!(matched.matched_chars >= "shared prefix ".chars().count());
    }

    #[test]
    fn recent_route_wins_same_prefix() {
        let mut index = CacheIndex::default();
        index.record("m", "a:m", "same prefix alpha", 16);
        index.record("m", "b:m", "same prefix beta", 16);

        let matched = index.match_prefix("m", "same prefix gamma").unwrap();

        assert_eq!(matched.route_id, "b:m");
    }

    #[test]
    fn route_records_are_bounded_and_evicted_from_tree() {
        let mut index = CacheIndex::default();
        index.record("m", "a:m", "old-only-prefix", 1);
        index.record("m", "a:m", "new-only-prefix", 1);

        let old = index.match_prefix("m", "old-only-prefix again");
        let new = index.match_prefix("m", "new-only-prefix again").unwrap();

        assert!(old.is_none());
        assert_eq!(new.route_id, "a:m");
        assert_eq!(index.route_stats("m", "a:m").records, 1);
        assert_eq!(index.stats().evictions_total, 1);
    }

    #[test]
    fn retain_model_routes_removes_disabled_routes() {
        let mut index = CacheIndex::default();
        index.record("m", "a:m", "alpha-only-prefix", 16);
        index.record("m", "b:m", "beta-only-prefix", 16);
        index.retain_model_routes("m", &HashSet::from_iter(["b:m".to_string()]));

        let matched = index.match_prefix("m", "alpha-only-prefix suffix");

        assert!(matched.is_none());
        assert_eq!(index.route_stats("m", "a:m").records, 0);
        assert_eq!(index.route_stats("m", "b:m").records, 1);
        assert_eq!(index.stats().removed_routes_total, 1);
    }

    #[test]
    fn utf8_prefix_counts_characters() {
        let mut index = CacheIndex::default();
        index.record("m", "a:m", "你好世界-a", 16);

        let matched = index.match_prefix("m", "你好世界-b").unwrap();

        assert_eq!(matched.route_id, "a:m");
        assert_eq!(matched.matched_chars, "你好世界-".chars().count());
    }
}
