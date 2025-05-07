use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Mutex;
use std::{marker::PhantomData, sync::OnceLock};

pub trait InnerMap<K, V>: Default {
    fn is_empty(&self) -> bool;
    fn len(&self) -> usize;
    fn contains_key<Q>(&self, k: &Q) -> bool
    where
        K: Borrow<Q> + Eq + Hash,
        Q: Eq + Hash + ?Sized;
}

pub trait ImmutableInnerMap<K, V>: InnerMap<K, V> {
    fn get<Q>(&self, k: &Q) -> Option<&V>
    where
        K: Borrow<Q> + Eq + Hash,
        Q: Eq + Hash + ?Sized;
}

pub trait MutableInnerMap<K, V>: InnerMap<K, V> {
    fn get<Q>(&self, k: &Q) -> Option<V>
    where
        K: Borrow<Q> + Eq + Hash,
        V: Clone,
        Q: Eq + Hash + ?Sized;
    fn insert(&self, k: K, v: V) -> Option<V>
    where
        K: Eq + Hash;
    fn remove<Q>(&self, k: &Q) -> Option<V>
    where
        K: Borrow<Q> + Eq + Hash,
        Q: Eq + Hash + ?Sized;
    fn clear(&self);
}

pub struct ShardMap<K, V, T: InnerMap<K, V> = HashMap<K, V>> {
    shards: Vec<T>,
    _phantom_data: PhantomData<(K, V)>,
}

pub type MutableShardMap<K, V> = ShardMap<K, V, Mutex<HashMap<K, V>>>;

fn default_shard_amount() -> usize {
    static DEFAULT_SHARD_AMOUNT: OnceLock<usize> = OnceLock::new();
    *DEFAULT_SHARD_AMOUNT.get_or_init(|| {
        (std::thread::available_parallelism().map_or(1, usize::from) * 4).next_power_of_two()
    })
}

impl<K: Eq + Hash, V, T: InnerMap<K, V>> Default for ShardMap<K, V, T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V, T: InnerMap<K, V>> ShardMap<K, V, T> {
    pub fn new() -> Self {
        let n = default_shard_amount();
        Self {
            shards: (0..n).map(|_| Default::default()).collect(),
            _phantom_data: Default::default(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.shards.iter().all(|m| m.is_empty())
    }

    pub fn len(&self) -> usize {
        self.shards.iter().map(|m| m.len()).sum::<usize>()
    }

    #[inline(always)]
    fn shard<Q>(&self, k: &Q) -> usize
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let mut s = DefaultHasher::new();
        k.hash(&mut s);
        s.finish() as usize % self.shards.len()
    }

    pub fn contains_key<Q>(&self, k: &Q) -> bool
    where
        K: Borrow<Q> + Eq + Hash,
        Q: Eq + Hash + ?Sized,
    {
        self.shards[self.shard(k)].contains_key(k)
    }
}

impl<K, V, T: ImmutableInnerMap<K, V>> ShardMap<K, V, T> {
    pub fn get<Q>(&self, k: &Q) -> Option<&V>
    where
        K: Borrow<Q> + Eq + Hash,
        Q: Eq + Hash + ?Sized,
    {
        self.shards[self.shard(k)].get(k)
    }
}

impl<K, V, T: MutableInnerMap<K, V>> ShardMap<K, V, T> {
    pub fn get_cloned<Q>(&self, k: &Q) -> Option<V>
    where
        K: Borrow<Q> + Eq + Hash,
        V: Clone,
        Q: Eq + Hash + ?Sized,
    {
        self.shards[self.shard(k)].get(k)
    }

    pub fn insert(&mut self, k: K, v: V) -> Option<V>
    where
        K: Eq + Hash,
    {
        let idx = self.shard(&k);
        self.shards[idx].insert(k, v)
    }

    pub fn remove<Q>(&mut self, k: &Q) -> Option<V>
    where
        K: Borrow<Q> + Eq + Hash,
        Q: Eq + Hash + ?Sized,
    {
        let idx = self.shard(k);
        self.shards[idx].remove(k)
    }

    pub fn clear(&self) {
        for shard in &self.shards {
            shard.clear();
        }
    }
}

impl<K, V> InnerMap<K, V> for HashMap<K, V> {
    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn contains_key<Q>(&self, k: &Q) -> bool
    where
        K: Borrow<Q> + Eq + Hash,
        Q: Eq + Hash + ?Sized,
    {
        self.contains_key(k)
    }
}

impl<K, V> ImmutableInnerMap<K, V> for HashMap<K, V> {
    fn get<Q>(&self, k: &Q) -> Option<&V>
    where
        K: Borrow<Q> + Eq + Hash,
        Q: Eq + Hash + ?Sized,
    {
        self.get(k)
    }
}

impl<K, V> InnerMap<K, V> for Mutex<HashMap<K, V>> {
    fn is_empty(&self) -> bool {
        let map = self.lock().unwrap();
        map.is_empty()
    }

    fn len(&self) -> usize {
        let map = self.lock().unwrap();
        map.len()
    }

    fn contains_key<Q>(&self, k: &Q) -> bool
    where
        K: Borrow<Q> + Eq + Hash,
        Q: Eq + Hash + ?Sized,
    {
        let map = self.lock().unwrap();
        map.contains_key(k)
    }
}

impl<K, V> MutableInnerMap<K, V> for Mutex<HashMap<K, V>> {
    fn get<Q>(&self, k: &Q) -> Option<V>
    where
        K: Borrow<Q> + Eq + Hash,
        V: Clone,
        Q: Eq + Hash + ?Sized,
    {
        let map = self.lock().unwrap();
        map.get(k).cloned()
    }

    fn insert(&self, k: K, v: V) -> Option<V>
    where
        K: Eq + Hash,
    {
        let mut map = self.lock().unwrap();
        map.insert(k, v)
    }

    fn remove<Q>(&self, k: &Q) -> Option<V>
    where
        K: Borrow<Q> + Eq + Hash,
        Q: Eq + Hash + ?Sized,
    {
        let mut map = self.lock().unwrap();
        map.remove(k)
    }

    fn clear(&self) {
        let mut map = self.lock().unwrap();
        map.clear()
    }
}

impl<K, V> From<MutableShardMap<K, V>> for ShardMap<K, V> {
    fn from(from: MutableShardMap<K, V>) -> Self {
        Self {
            shards: from
                .shards
                .into_iter()
                .map(|v| v.into_inner().unwrap())
                .collect(),
            _phantom_data: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shard_map() {
        let mut map = MutableShardMap::<usize, usize>::default();

        assert!(map.is_empty());
        map.insert(1, 1);
        assert_eq!(map.len(), 1);
        map.clear();
        assert!(map.is_empty());
        assert_eq!(map.len(), 0);

        const N: usize = 1 << 15;
        for i in 0..N {
            assert!(map.insert(i, i * 2).is_none());
        }
        for i in 0..N {
            if i % 2 == 0 {
                assert_eq!(map.remove(&i), Some(i * 2));
            } else {
                assert_eq!(map.get_cloned(&i), Some(i * 2));
            }
        }
        assert!(!map.contains_key(&0));
        assert!(map.contains_key(&1));

        let immutable_map: ShardMap<usize, usize> = map.into();
        assert!(!immutable_map.is_empty());
        assert_eq!(immutable_map.len(), N / 2);
        for i in 0..N {
            if i % 2 == 1 {
                assert_eq!(immutable_map.get(&i).cloned(), Some(i * 2));
            }
        }
        assert!(immutable_map.contains_key(&1));
    }
}
