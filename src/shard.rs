use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
    ops::Rem,
};

#[derive(Debug)]
pub struct Sharded<T: Default> {
    size: u8,
    inner: HashMap<u8, T>,
}

impl<T: Default> Sharded<T> {
    pub fn new(size: u8) -> Self {
        assert!(size > 0);
        let mut inner = HashMap::new();
        for i in 0..size {
            inner.insert(i, T::default());
        }
        Self { size, inner }
    }

    pub fn get<K: Hash>(&self, key: &K) -> &T {
        let mut h = std::collections::hash_map::DefaultHasher::default();
        key.hash(&mut h);
        let hash = h.finish();
        let i = hash.rem(self.size as u64) as u8;
        self.inner.get(&i).unwrap()
    }
}

impl<'a, T: Default> IntoIterator for &'a Sharded<T> {
    type Item = &'a T;
    type IntoIter = std::collections::hash_map::Values<'a, u8, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.values()
    }
}

#[test]
fn sharded_test() {
    use std::sync::Mutex;
    let s = Sharded::<Mutex<Vec<u8>>>::new(2);
    let key = "asd";
    assert_eq!(Vec::<u8>::new(), *s.get(&key).lock().unwrap());
    s.get(&key).lock().unwrap().push(1);
    assert_eq!(vec![1u8], *s.get(&key).lock().unwrap());
}
