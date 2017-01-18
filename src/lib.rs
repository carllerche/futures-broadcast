#[macro_use]
extern crate futures;

mod atomic_task;

use atomic_task::AtomicTask;

use futures::{Stream, Sink, Poll, StartSend, Async, AsyncSink};
use futures::task::{self, Task};

use std::{ops, mem, ptr, usize};
use std::cell::UnsafeCell;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, AtomicPtr};
use std::sync::atomic::Ordering::{Acquire, Release, AcqRel, Relaxed};

// The core algorithm is based on the mpmc array channel from 1024 cores.
//
// * When a sender "blocks", it inserts it's "task" node into the entry
// * When a receiver "blocks", it inserts it's task node into the entry
//
// # Guarantees
//
// When a TX is working on an entry, there will never be another TX which
// will operate on the same entry. This means that, as long as the `TX` is
// in the `send` phase, there will never be other TX waiters.
//
// When an RX is working on an entry, it is not possible for the sequence to
// cycle as all RX handles must complete "seeing" the value.
//
// It *is* possible for all RX handles to see a value before the TX send fn
// completes.
//
// # Notes
//
// Separate TX & RX wait stacks are necessary as there could be races between a
// TX trying to wait, and the RX pushing a value & another RX getting blocked on
// the full entry.
//
// # Entry states
//
// * EMPTY
// * EMPTY_WAITERS
// * FULL - TX puts value from EMPTY state
// * FULL_WAITERS - RX waiting for slot, includes value
//
// # Events
//
// 1 - RX calls `recv` and encounters `EMPTY` state.
//   1.1 - Push waiter onto RX wait stack.
//   1.2 - State `EMPTY` -> `EMPTY_WAITERS`
//      1.2.1 - Fail w/ `FULL`, take waiters & notify if not self
//      1.2.2 - Fail w/ `EMPTY_WAITERS`, do nothing
//      1.2.3 - Success, return
//
// 2 - RX calls `recv` w/ `EMPTY_WAITERS` state.
//    2.1 - Push waiter onto RX wait stack.
//    2.2 - Confirm state `EMPTY_WAITERS`
//      2.2.1 - Success, return
//      2.2.2 - Fail w/ `FULL`, take waiters & notify if not self
//
// 3 - TX sends value, `EMPTY` state
//   3.1 - Set value
//   3.2 - Transition `EMPTY` -> `FULL`
//      3.2.1 - Fail, state must be `EMPTY_WAITERS` GOTO 4.2
//      3.2.2 - Success, no other work
//
// 4 - TX sends value, `EMPTY_WAITERS` state
//   4.1 - Set value
//   4.2 - Transition `EMPTY_WAITERS` -> `FULL` (must succeed)
//      4.2.1 - Take all waiters & notify
//
//
// # Sequence state
//
// `0` - Entry empty
// `1` - Entry full
// `2` - Empty with waiting subscribers
// `3` - Full with waiting publisher
//
// # Limitations
//
// * The minimum capacity must be 4 in order to allow enough lower bits in the
//   sequence number to store the entry state.
//
// * The max number of outstanding senders must be less than or equal to the
//   channel capacity.

pub struct Sender<T> {
    inner: Arc<Inner<T>>,
}

pub struct Receiver<T> {
    inner: Arc<Inner<T>>,
    pos: usize,
    waiter: Option<Arc<WaitingRx>>,
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

    // Number of outstanding senders
    num_tx: AtomicUsize,
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

    // Head of the `WaitingRx` stack
    waiting_rx: AtomicPtr<WaitingRx>,

    // Pointer to a waiting TX node.
    waiting_tx: AtomicPtr<WaitingTx<T>>,
}

// Used to track a waiter. Node in a linked-list.
struct WaitingRx {
    // Parked task
    task: AtomicTask,
    // Next waiter
    next: AtomicPtr<WaitingRx>,
}

struct WaitingTx<T> {
    // Parked task
    task: AtomicTask,

    // queued value
    value: T,
}

// Used as masks
const FULL: usize = 1;

/// Returns a channel
pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let inner = Arc::new(Inner::with_capacity(capacity));

    let tx = Sender {
        inner: inner.clone(),
    };

    let rx = Receiver {
        inner: inner,
        pos: 0,
        waiter: None,
    };

    (tx, rx)
}

// ===== impl Sender =====

impl<T> Sender<T> {
    /// Try to clone the `Sender`. This will fail if there are too many
    /// outstanding senders.
    pub fn try_clone(&self) -> Result<Self, ()> {
        let mut curr = self.inner.num_tx.load(Relaxed);

        loop {
            if curr == self.inner.buffer.len() {
                return Err(());
            }

            let actual = self.inner.num_tx.compare_and_swap(curr, curr + 1, Relaxed);

            if actual == curr {
                return Ok(Sender {
                    inner: self.inner.clone(),
                });
            }

            curr = actual;
        }
    }
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

            if seq == pub_state.pos {
                // The slot is available, we can attempt to acquire the slot
                match self.inner.pub_state.claim_slot(pub_state) {
                    Ok(_) => {
                        // CAS succeeded, update the value
                        entry.set(item, pub_state);

                        let color = color_for(pub_state.pos, mask);

                        // Notify receivers
                        entry.notify_rx(color);

                        return Ok(AsyncSink::Ready);
                    }
                    Err(actual_state) => {
                        // CAS failed, try again
                        pub_state = actual_state;
                    }
                }
            } else if seq < pub_state.pos {
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
            waiter: None,
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

            let diff: isize = (seq as isize).wrapping_sub((pos + 1) as isize);

            if diff == 0 {
                // The slot is full, get a reference to the value.
                unsafe {
                    (*entry.value.get()).as_ref().unwrap()
                }
            }
            else {
                // No value present, attempt to wait
                //
                // First, get a waiter. `rx_waiter` ensures that the
                // `Receiver`'s wait node references the current task.
                let waiter = rx_waiter(&mut self.waiter);

                // Push the node onto the stack, returns `false` if the TX half
                // has "terminated" the stack indicating that a value is now
                // available.
                let color = color_for(pos, mask);

                if push_waiter(&entry.waiting_rx, waiter, color) {
                    // The wait has been successfully issued, now return w/
                    // NotReady
                    return Ok(Async::NotReady);
                }

                // Pushing the node failed, this implies that the TX half
                // "shutdown" the wait queue, transitioning to `FULL`.
                //
                // `Relaxed` ordering is used here as the memory ordering is
                // actually established in `push_node`.

                let seq = entry.sequence.load(Relaxed);
                let diff: isize = (seq as isize).wrapping_sub((pos + 1) as isize);

                if diff == 0 {
                    // The entry is full, get a reference to the value.
                    unsafe {
                        (*entry.value.get()).as_ref().unwrap()
                    }
                } else {
                    // The entry is not full, meaning that all TX handles were
                    // dropped.
                    unimplemented!();
                }
            }
        };

        // The slot is ready to be read
        Ok(Async::Ready(Some(RecvGuard {
            recv: self,
            value: value,
        })))
    }
}

fn rx_waiter(cell: &mut Option<Arc<WaitingRx>>) -> Arc<WaitingRx> {
    if let Some(ref w) = *cell {
        // Concurrent calls to `AtomicTask::park()` are guaranteed by having a
        // &mut reference to the cell.
        unsafe { w.task.park() };

        return w.clone();
    }

    let w = Arc::new(WaitingRx {
        task: AtomicTask::new(task::park()),
        next: AtomicPtr::new(ptr::null_mut()),
    });

    *cell = Some(w.clone());
    w
}

// TODO: Move this to a fn on `Entry`
fn push_waiter(head: &AtomicPtr<WaitingRx>, node: Arc<WaitingRx>, target_color: usize) -> bool {
    // Push a waiter node onto the atomic wait stack.
    unsafe {
        // Only push nodes without a `next` pointer
        if !node.next.load(Acquire).is_null() {
            // Task is currently queued
            return true;
        }

        let mut curr = head.load(Acquire);

        // Transmute the Arc<WaitingRx> -> an unsafe ptr. This ptr will be
        // stored in the AtomicPtr stack
        let node_ptr: *mut WaitingRx = mem::transmute(node);

        loop {
            let (head_ptr, color) = atomic_to_ptr_and_color(curr);

            if color == target_color {
                // The wait stack is closed. The node will not be CASed to the
                // stack, so transmute back to the Arc so that the refcount is
                // decremented and unset the `next` node before the value is
                // dropped.
                let _: Arc<WaitingRx> = mem::transmute(node_ptr);

                return false;
            }

            // Update next pointer.
            (*node_ptr).next.store(head_ptr, Relaxed);

            let next = ptr_and_color_to_atomic(node_ptr, color);
            let actual = head.compare_and_swap(curr, next, Release);

            if actual == curr {
                return true;
            }

            curr = actual;
        }
    }
}

/// Splits the value in the `AtomicPtr` into the actual pointer and the color
fn atomic_to_ptr_and_color<T>(ptr: *mut T) -> (*mut T, usize) {
    let ptr = ptr as usize;
    let color = ptr & 1;
    let ptr = (ptr & !1) as *mut T;

    (ptr, color)
}

/// Combines a pointer and a color into a single pointer to store in the
/// `AtomicPtr`
fn ptr_and_color_to_atomic<T>(ptr: *mut T, color: usize) -> *mut T {
    let ptr = ptr as usize | color;
    ptr as *mut T
}

/// Takes a `pos` and a `mask` and returns the associated color
fn color_for(pos: usize, mask: usize) -> usize {
    let mask = (mask << 1) & (!mask);

    if pos & mask == 0 {
        0
    } else {
        1
    }
}

/*
/// Returns a "closed" token
fn closed<T>() -> *mut T {
    unsafe { mem::transmute(1usize) }
}
*/

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

        // Initialize the waiting_rx, setting the color to `1`
        let waiting_rx = ptr_and_color_to_atomic(ptr::null_mut(), 1);

        for i in 0..capacity {
            buffer.push(Entry {
                sequence: AtomicUsize::new(i),
                value: UnsafeCell::new(None),
                remaining: AtomicUsize::new(0),
                waiting_rx: AtomicPtr::new(waiting_rx),
                waiting_tx: AtomicPtr::new(ptr::null_mut()),
            });
        }

        Inner {
            buffer: buffer,
            mask: capacity - 1,
            pub_state: PubCell::new(),
            num_tx: AtomicUsize::new(1),
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

    fn notify_rx(&self, color: usize) {
        let closed_ptr = ptr_and_color_to_atomic(ptr::null_mut(), color);

        // Take the stack of waiters
        //
        // Acquire the waiting_rx state and release the "set value" state.
        let curr = self.waiting_rx.swap(closed_ptr, AcqRel);

        let (mut ptr, _) = atomic_to_ptr_and_color(curr);

        while !ptr.is_null() {
            unsafe {
                let node: Arc<WaitingRx> = mem::transmute(ptr);

                // Unpark the task
                node.task.unpark();

                ptr = node.next.swap(ptr::null_mut(), Release);
            }
        }
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
