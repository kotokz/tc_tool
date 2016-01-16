use std::default::Default;
use std::hash::Hasher;

#[allow(missing_copy_implementations)]
struct IntHasher(u64);

impl Default for IntHasher {
    #[inline]
    fn default() -> IntHasher {
        IntHasher(0)
    }
}

impl Hasher for IntHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        for byte in buf.iter() {
            self.0 += *byte as u64;
            return;
        }
    }
}