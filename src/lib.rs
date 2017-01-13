#[macro_use]
extern crate futures;

use futures::{Stream, Sink, Poll, StartSend, Async, AsyncSink};
use futures::task::{self, Task};

use std::{ops, usize};
use std::cell::UnsafeCell;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize};
use std::sync::atomic::Ordering::{Acquire, Release, Relaxed};

pub struct Sender<T> {
    inner: Arc<Inner<T>>,
}

pub struct Receiver<T> {
    inner: Arc<Inner<T>>,
    pos: usize,
}

pub struct RecvGuard<'a, T: 'a> {
    recv: &'a mut Receiver<T>,
    value: &'a T,
}

struct Inner<T> {
    // Pre-allocated buffer of entries
    buffer: Vec<Entry<T>>,

    // Buffer access mask
    mask: usize,

    // Used by publishers and when receivers are cloned.
    pub_state: PubCell,
}

// Contains the `PubState`
//
// Currently, this is coordinated with a `Mutex`, however there are a bunch of
// other strategies that could be used.
struct PubCell {
    pub_state: Mutex<PubState>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct PubState {
    // Current producer position
    pos: usize,

    // Number of outstanding receiver handles
    num_rx: usize,
}

struct Entry<T> {
    // Atomic channel sequence number
    sequence: AtomicUsize,

    // Number of remaining receivers to observe the value
    remaining: AtomicUsize,

    // Value being published
    value: UnsafeCell<Option<T>>,
}

/// Returns a channel
pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let inner = Arc::new(Inner::with_capacity(capacity));

    let tx = Sender {
        inner: inner.clone(),
    };

    let rx = Receiver {
        inner: inner,
        pos: 0,
    };

    (tx, rx)
}

// ===== impl Sender =====

impl<T> Sender<T> {
}

impl<T> Sink for Sender<T> {
    type SinkItem = T;
    type SinkError = ();

    fn start_send(&mut self, item: T) -> StartSend<T, ()> {
        let mask = self.inner.mask;
        let mut pub_state = self.inner.pub_state.load();

        loop {
            let entry = &self.inner.buffer[pub_state.pos & mask];
            let seq = entry.sequence.load(Acquire);
            let diff: isize = seq as isize - pub_state.pos as isize;

            if diff == 0 {
                // The slot is available, we can attempt to acquire the slot
                match self.inner.pub_state.claim_slot(pub_state) {
                    Ok(_) => {
                        // CAS succeeded, update the value
                        entry.set(item, pub_state);

                        // TODO: Wakeup subscribers
                        return Ok(AsyncSink::Ready);
                    }
                    Err(actual_state) => {
                        // CAS failed, try again
                        pub_state = actual_state;
                    }
                }
            } else if diff < 0 {
                // Full
                unimplemented!();
            } else {
                // Try again
                pub_state = self.inner.pub_state.load();
            }
        }
    }

    fn poll_complete(&mut self) -> Poll<(), ()> {
        Ok(Async::Ready(()))
    }
}

// ===== impl Receiver =====

impl<T> Receiver<T> {

    /// Returns a new receiver positioned at the head of the channel
    pub fn new_receiver(&self) -> Receiver<T> {
        let pos = self.inner.pub_state.inc_rx();

        Receiver {
            inner: self.inner.clone(),
            pos: pos,
        }
    }

    pub fn recv(&mut self) -> Poll<Option<RecvGuard<T>>, ()> {
        let mask = self.inner.mask;
        let pos = self.pos;

        // A little bit of misdirection to make the borrow checker happy.
        let value = {
            // Get the entry at the current position
            let entry = &self.inner.buffer[pos & mask];

            // Get the sequence number
            let seq = entry.sequence.load(Acquire);
            let diff: isize = seq as isize - (pos + 1) as isize;

            if diff == 0 {
                unsafe {
                    (*entry.value.get()).as_ref().unwrap()
                }
            } else if diff < 0 {
                // unimplemented!();
                return Err(());
            } else {
                unimplemented!();
            }
        };

        // The slot is ready to be read
        Ok(Async::Ready(Some(RecvGuard {
            recv: self,
            value: value,
        })))
    }
}

impl<T: Clone> Stream for Receiver<T> {
    type Item = T;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<T>, ()> {
        match try_ready!(self.recv()) {
            Some(val) => Ok(Async::Ready(Some(val.clone()))),
            None => Ok(Async::Ready(None)),
        }
    }
}

// ===== impl Inner =====

impl<T> Inner<T> {
    fn with_capacity(capacity: usize) -> Inner<T> {
        // TODO: Capacity is capped at a smaller number than usize::MAX
        //
        let capacity = if capacity < 2 || (capacity & (capacity - 1)) != 0 {
            if capacity < 2 {
                2
            } else {
                // use next power of 2 as capacity
                capacity.next_power_of_two()
            }
        } else {
            capacity
        };

        // Initialize the ring as a vec of vaccant entries
        let mut buffer = Vec::with_capacity(capacity);

        for i in 0..capacity {
            buffer.push(Entry {
                sequence: AtomicUsize::new(i),
                value: UnsafeCell::new(None),
                remaining: AtomicUsize::new(0),
            });
        }

        Inner {
            buffer: buffer,
            mask: capacity - 1,
            pub_state: PubCell::new(),
        }
    }
}

unsafe impl<T: Send + Sync> Send for Inner<T> {}
unsafe impl<T: Send + Sync> Sync for Inner<T> {}

// ===== impl PubCell =====

impl PubCell {
    fn new() -> PubCell {
        PubCell {
            pub_state: Mutex::new(PubState {
                pos: 0,
                num_rx: 1,
            }),
        }
    }

    // Loads the state
    fn load(&self) -> PubState {
        let state = self.pub_state.lock().unwrap();
        *state
    }

    fn claim_slot(&self, expect: PubState) -> Result<(), PubState> {
        let mut state = self.pub_state.lock().unwrap();

        if *state == expect {
            state.pos += 1;
            Ok(())
        } else {
            Err(*state)
        }
    }

    fn inc_rx(&self) -> usize {
        let mut state = self.pub_state.lock().unwrap();

        if state.num_rx == usize::MAX {
            panic!();
        }

        state.num_rx += 1;
        state.pos
    }
}

// ===== impl Entry =====

impl<T> Entry<T> {
    fn set(&self, item: T, pub_state: PubState) {
        unsafe {
            // Set the value
            (*self.value.get()) = Some(item);
        }

        // Set the number of remaining subscribers to observe
        self.remaining.store(pub_state.num_rx, Relaxed);

        // Store the sequence number, which makes the entry visible to
        // subscribers
        self.sequence.store(pub_state.pos + 1, Release);
    }
}

// ===== impl RecvGuard =====

impl<'a, T> ops::Deref for RecvGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &*self.value
    }
}

impl<'a, T> Drop for RecvGuard<'a, T> {
    fn drop(&mut self) {
        // It's critical to not use the `value` pointer in here.

        let mask = self.recv.inner.mask;
        let pos = self.recv.pos;

        // Get the entry at the current position
        let entry = &self.recv.inner.buffer[pos & mask];

        // Decrement the remaining receivers
        if 1 == entry.remaining.fetch_sub(1, Release) {
            // Remove the value
            unsafe { (*entry.value.get()) = None };

            // Update the entry sequence value, this makes the slot available to
            // producers.
            entry.sequence.store(pos + mask + 1, Release);

            // TODO: Notify producers
        }

        // Increment the position
        self.recv.pos += 1;
    }
}
