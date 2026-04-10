use std::sync::{Arc, RwLock};

use lru::LruCache;

/// LRU cache that evicts based on total memory usage rather than item count.
pub struct SizedLruCache<K: std::hash::Hash + Eq + Clone, V> {
    entries: LruCache<K, V>,
    sizes: LruCache<K, usize>,
    current_bytes: usize,
    max_bytes: usize,
}

impl<K: std::hash::Hash + Eq + Clone, V> SizedLruCache<K, V> {
    pub fn new(max_bytes: usize) -> Self {
        SizedLruCache {
            entries: LruCache::unbounded(),
            sizes: LruCache::unbounded(),
            current_bytes: 0,
            max_bytes,
        }
    }

    pub fn get(&mut self, key: &K) -> Option<&V> {
        self.sizes.promote(key);
        self.entries.get(key)
    }

    pub fn put(&mut self, key: K, value: V, size: usize) {
        if let Some(old_size) = self.sizes.pop(&key) {
            self.entries.pop(&key);
            self.current_bytes -= old_size;
        }

        while self.current_bytes + size > self.max_bytes {
            if let Some((evicted_key, evicted_size)) = self.sizes.pop_lru() {
                self.entries.pop(&evicted_key);
                self.current_bytes -= evicted_size;
            } else {
                break;
            }
        }

        self.entries.put(key.clone(), value);
        self.sizes.put(key, size);
        self.current_bytes += size;
    }

    pub fn contains(&self, key: &K) -> bool {
        self.entries.contains(key)
    }
}

/// 2 MB budget for rendered row strings
const RENDERED_CACHE_BUDGET: usize = 2 * 1024 * 1024;

use crate::render::RenderedRow;

/// Thread-safe rendered-row cache, keyed by global row index.
pub struct RowCache {
    inner: RwLock<SizedLruCache<usize, Arc<RenderedRow>>>,
}

impl RowCache {
    pub fn new() -> Self {
        RowCache {
            inner: RwLock::new(SizedLruCache::new(RENDERED_CACHE_BUDGET)),
        }
    }

    pub fn get(&self, row: usize) -> Option<Arc<RenderedRow>> {
        self.inner.write().ok()?.get(&row).cloned()
    }

    pub fn put(&self, row: usize, rendered: RenderedRow) {
        let size = rendered.byte_size();
        if let Ok(mut cache) = self.inner.write() {
            cache.put(row, Arc::new(rendered), size);
        }
    }

    pub fn contains(&self, row: usize) -> bool {
        self.inner.read().ok().map_or(false, |c| c.contains(&row))
    }
}
