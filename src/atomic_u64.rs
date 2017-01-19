#![allow(dead_code)]

pub use self::imp::AtomicU64;

#[cfg(target_pointer_width = "64")]
mod imp {
    use std::sync::atomic::{AtomicUsize, Ordering};

    pub struct AtomicU64 {
        v: AtomicUsize,
    }

    impl AtomicU64 {
        pub fn new(v: u64) -> AtomicU64 {
            AtomicU64 { v: AtomicUsize::new(v as usize) }
        }

        pub fn load(&self, order: Ordering) -> u64 {
            self.v.load(order) as u64
        }

        pub fn store(&self, val: u64, order: Ordering) {
            self.v.store(val as usize, order);
        }

        pub fn swap(&self, val: u64, order: Ordering) -> u64 {
            self.v.swap(val as usize, order) as u64
        }

        pub fn compare_and_swap(&self, old: u64, new: u64, order: Ordering) -> u64 {
            self.v.compare_and_swap(old as usize, new as usize, order) as u64
        }

        pub fn fetch_add(&self, val: u64, order: Ordering) -> u64 {
            self.v.fetch_add(val as usize, order) as u64
        }

        pub fn fetch_sub(&self, val: u64, order: Ordering) -> u64 {
            self.v.fetch_sub(val as usize, order) as u64
        }

        pub fn fetch_and(&self, val: u64, order: Ordering) -> u64 {
            self.v.fetch_and(val as usize, order) as u64
        }

        pub fn fetch_or(&self, val: u64, order: Ordering) -> u64 {
            self.v.fetch_or(val as usize, order) as u64
        }

        pub fn fetch_xor(&self, val: u64, order: Ordering) -> u64 {
            self.v.fetch_xor(val as usize, order) as u64
        }
    }
}

#[cfg(not(target_pointer_width = "64"))]
mod imp {
    #![allow(unused_variables)] // order is not used

    use std::sync::Mutex;
    use std::sync::atomic::Ordering;

    pub struct AtomicU64 {
        v: Mutex<u64>,
    }

    impl AtomicU64 {
        pub fn new(v: u64) -> AtomicU64 {
            AtomicU64 { v: Mutex::new(v) }
        }

        pub fn load(&self, order: Ordering) -> u64 {
            *self.v.lock().unwrap()
        }

        pub fn store(&self, val: u64, order: Ordering) {
            *self.v.lock().unwrap() = val
        }

        pub fn swap(&self, val: u64, order: Ordering) -> u64 {
            let mut lock = self.v.lock().unwrap();
            let prev = *lock;
            *lock = val;
            prev
        }

        pub fn compare_and_swap(&self, old: u64, new: u64, order: Ordering) -> u64 {
            let mut lock = self.v.lock().unwrap();
            let prev = *lock;

            if prev != old {
                return prev;
            }

            *lock = new;
            prev
        }

        pub fn fetch_add(&self, val: u64, order: Ordering) -> u64 {
            let mut lock = self.v.lock().unwrap();
            let prev = *lock;
            *lock = prev + val;
            prev
        }

        pub fn fetch_sub(&self, val: u64, order: Ordering) -> u64 {
            let mut lock = self.v.lock().unwrap();
            let prev = *lock;
            *lock = prev - val;
            prev
        }

        pub fn fetch_and(&self, val: u64, order: Ordering) -> u64 {
            let mut lock = self.v.lock().unwrap();
            let prev = *lock;
            *lock = prev & val;
            prev
        }

        pub fn fetch_or(&self, val: u64, order: Ordering) -> u64 {
            let mut lock = self.v.lock().unwrap();
            let prev = *lock;
            *lock = prev | val;
            prev
        }

        pub fn fetch_xor(&self, val: u64, order: Ordering) -> u64 {
            let mut lock = self.v.lock().unwrap();
            let prev = *lock;
            *lock = prev ^ val;
            prev
        }
    }
}
