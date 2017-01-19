#[macro_use]
extern crate futures;

mod atomic_task;
mod atomic_u64;

use atomic_task::AtomicTask;
use atomic_u64::AtomicU64;

use futures::{Stream, Sink, Poll, StartSend, Async, AsyncSink};
use futures::task::{self, Task};

use std::{ops, mem, ptr, u32, usize};
use std::cell::UnsafeCell;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, AtomicUsize, AtomicPtr};
use std::sync::atomic::Ordering::{self, Acquire, Release, AcqRel, Relaxed};

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

pub struct Sender<T> {
    inner: Arc<Inner<T>>,
    waiter: Option<Arc<WaitingTx<T>>>,
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

    // Sequence value mask
    seq_mask: usize,

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
    pub_state: AtomicU64,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct PubState {
    // Current producer position
    pos: u32,

    // Number of outstanding receiver handles
    num_rx: u32,
}

struct Entry<T> {
    // Value being published
    value: UnsafeCell<Option<T>>,

    // Stores the entry state. This is a combination of the necessary flags to
    // track the state as well as a pointer to the head of the waiting stack.
    state: StateCell,

    // Number of remaining receivers to observe the value
    remaining: AtomicUsize,

    // Head of the `WaitingRx` stack
    // waiting_rx: AtomicPtr<WaitingRx>,

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
    value: UnsafeCell<Option<T>>,

    // True if parked
    parked: AtomicBool,
}

struct StateCell {
    state: AtomicUsize,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
struct State {
    // Pointer to the head of the waiting RXs
    waiting_rx: *mut WaitingRx,

    // The least significant bit of number of types the channel buffer has been
    // cycled.
    sequence: usize,

    // TX and RX halves race to toggle this flag. If TX wins, then the slot is
    // made available to an RX, if an RX wins, then the RX is blocked.
    toggled: bool,
}

/// Returns a channel
pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let inner = Arc::new(Inner::with_capacity(capacity));

    let tx = Sender {
        inner: inner.clone(),
        waiter: None,
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
                    waiter: None,
                });
            }

            curr = actual;
        }
    }

    fn is_parked(&self) -> bool {
        match self.waiter {
            Some(ref w) => w.parked.load(Acquire),
            None => false,
        }
    }
}

impl<T> Sink for Sender<T> {
    type SinkItem = T;
    type SinkError = ();

    fn start_send(&mut self, item: T) -> StartSend<T, ()> {
        if self.is_parked() {
            return Ok(AsyncSink::NotReady(item));
        }

        let mask = self.inner.mask;
        let seq_mask = self.inner.seq_mask;

        // Claim a lot, this does not need to be guarded like in the original
        // MPMC queue because the size restrictions on the channel and the
        // `is_parked` is a sufficient guard.
        let pub_state = self.inner.pub_state.claim_slot();

        // Get the sequence value for the given position
        let seq = pos_to_sequence(pub_state.pos(), seq_mask);

        // Get a handle to the entry
        let entry = &self.inner.buffer[pub_state.pos() & mask];

        // Load the current entry state
        let mut entry_state = entry.state.load(Acquire);

        loop {
            debug_assert!(seq != entry_state.sequence);

            if entry_state.toggled {
                // The subscriber has released this slot and the publisher may
                // write a value to it.
                entry.set(item, pub_state);

                // A CAS loop for updating the entry state.
                loop {
                    let next = State {
                        waiting_rx: ptr::null_mut(),
                        sequence: seq,
                        toggled: false,
                        .. entry_state
                    };

                    let actual = entry.state.compare_and_swap(entry_state, next, Release);

                    if actual == entry_state {
                        break;
                    }

                    entry_state = actual;
                }

                // Notify waiters
                entry_state.notify_rx();

                return Ok(AsyncSink::Ready);

            } else {
                unimplemented!();
            }
        }
    }

    fn poll_complete(&mut self) -> Poll<(), ()> {
        Ok(Async::Ready(()))
    }
}

fn pos_to_sequence(pos: usize, mask: usize) -> usize {
    if pos & mask == 0 {
        0
    } else {
        1
    }
}

/*
fn tx_waiter<T>(cell: &mut Option<Arc<WaitingTx<T>>>, item: T) -> Arc<WaitingTx<T>> {
    if let Some(ref w) = *cell {
        unsafe {
            w.task.park();

            (*w.value.get()) = Some(item);
        }

        w.parked.store(true, Release);

        return w.clone();
    }

    let w = Arc::new(WaitingTx {
        task: AtomicTask::new(task::park()),
        value: UnsafeCell::new(Some(item)),
        parked: AtomicBool::new(true),
    });

    *cell = Some(w.clone());
    w
}
*/

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let prev = self.inner.num_tx.fetch_sub(1, AcqRel);

        if prev == 1 {
            // TODO: implement notifying
        }
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
        let seq_mask = self.inner.seq_mask;
        let pos = self.pos;

        // A little bit of misdirection to make the borrow checker happy.
        let value = {
            // Get the entry at the current position
            let entry = &self.inner.buffer[pos & mask];
            let mut entry_state = entry.state.load(Acquire);

            let seq = pos_to_sequence(pos, seq_mask);

            if seq != entry_state.sequence {
                // No value present, attempt to wait
                //
                // First, get a waiter. `rx_waiter` ensures that the
                // `Receiver`'s wait node references the current task.
                let waiter = rx_waiter(&mut self.waiter);

                // Transmute the waiter to an unsafe pointer. This ptr will
                // be stored in the state slot
                let node_ptr: *mut WaitingRx = unsafe { mem::transmute(waiter) };

                loop {
                    // Update the next pointer.
                    //
                    // Relaxed ordering is used as the waiter node is not
                    // currently shared with other threads.
                    unsafe {
                        (*node_ptr).next.store(entry_state.waiting_rx, Relaxed);
                    }

                    let next = State {
                        waiting_rx: node_ptr,
                        .. entry_state
                    };

                    // Attempt to CAS the new state
                    let actual = entry.state.compare_and_swap(entry_state, next, AcqRel);

                    if actual == entry_state {
                        // The wait has successfully been registered, so return
                        // with NotReady
                        return Ok(Async::NotReady);
                    }

                    // The CAS failed, maybe the value is not ready. This is why
                    // Acq is used in the CAS.
                    if seq == entry_state.sequence {
                        // The Arc must be cleaned up...
                        let _: Arc<WaitingRx> = unsafe { mem::transmute(node_ptr) };
                        break;
                    }

                    // The CAS failed for another reason, update the state and
                    // try again.
                    entry_state = actual;
                }
            }

            // Read the value
            unsafe {
                (*entry.value.get()).as_ref().unwrap()
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
                state: StateCell::new(),
                value: UnsafeCell::new(None),
                remaining: AtomicUsize::new(0),
                waiting_tx: AtomicPtr::new(ptr::null_mut()),
            });
        }

        let mask = capacity - 1;

        Inner {
            buffer: buffer,
            mask: mask,
            seq_mask: (mask << 1) & (!mask),
            pub_state: PubCell::new(),
            num_tx: AtomicUsize::new(1),
        }
    }
}

unsafe impl<T: Send + Sync> Send for Sender<T> {}
unsafe impl<T: Send + Sync> Sync for Sender<T> {}

unsafe impl<T: Send + Sync> Send for Inner<T> {}
unsafe impl<T: Send + Sync> Sync for Inner<T> {}

// ===== impl PubCell =====

impl PubCell {
    fn new() -> PubCell {
        let v = PubState::new().as_u64();

        PubCell { pub_state: AtomicU64::new(v) }
    }

    // Loads the state
    fn load(&self, ordering: Ordering) -> PubState {
        let val = self.pub_state.load(ordering);
        PubState::load(val)
    }

    fn compare_and_swap(&self, current: PubState, new: PubState, ordering: Ordering) -> PubState {
        let val = self.pub_state.compare_and_swap(current.as_u64(), new.as_u64(), ordering);
        PubState::load(val)
    }

    /// Claim the next publish spot and return the new `PubState`
    ///
    /// Uses `Relaxed` ordering
    fn claim_slot(&self) -> PubState {
        let mut curr = self.load(Relaxed);

        loop {
            let next = PubState {
                pos: curr.pos.wrapping_add(1),
                .. curr
            };

            let actual = self.compare_and_swap(curr, next, Relaxed);

            if curr == actual {
                return curr;
            }

            curr = actual
        }
    }

    /// Atomically increment the number of outstanding RX handles and return the
    /// current "head" position.
    ///
    /// Uses `Relaxed` ordering
    fn inc_rx(&self) -> usize {
        let mut curr = self.load(Relaxed);

        loop {
            if curr.num_rx == u32::MAX {
                // TODO: return this as an error
                panic!();
            }

            let next = PubState {
                num_rx: curr.num_rx + 1,
                .. curr
            };

            let actual = self.compare_and_swap(curr, next, Relaxed);

            if curr == actual {
                return next.pos();
            }

            curr = actual;
        }
    }
}

// ===== impl PubState =====

impl PubState {
    /// Return a new `PubState` with default values
    fn new() -> PubState {
        PubState {
            pos: 0,
            num_rx: 1,
        }
    }

    fn pos(&self) -> usize {
        self.pos as usize
    }

    fn num_rx(&self) -> usize {
        self.num_rx as usize
    }

    /// Load a `PubState` from its u64 representation
    fn load(val: u64) -> PubState {
        PubState {
            pos: (val >> 32) as u32,
            num_rx: (val & (u32::MAX as u64)) as u32,
        }
    }

    /// Return the u64 representation for this `PubState`
    fn as_u64(&self) -> u64 {
        ((self.pos as u64) << 32) | (self.num_rx as u64)
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
        //
        // The `Relaxed` ordering is sufficient here as all receivers with
        // `Acquire` this memory when loading the entry state.
        self.remaining.store(pub_state.num_rx(), Relaxed);
    }
}

// ===== impl StateCell =====

impl StateCell {
    fn new() -> StateCell {
        let val = State::new().as_usize();

        StateCell {
            state: AtomicUsize::new(val),
        }
    }

    fn load(&self, ordering: Ordering) -> State {
        let val = self.state.load(ordering);
        State::load(val)
    }

    fn compare_and_swap(&self, current: State, next: State, ordering: Ordering) -> State {
        let val = self.state.compare_and_swap(current.as_usize(), next.as_usize(), ordering);
        State::load(val)
    }
}

// ===== impl State =====

impl State {
    /// Return a new `State` value
    fn new() -> State {
        State {
            waiting_rx: ptr::null_mut(),
            sequence: 1,
            toggled: true,
        }
    }

    /// Load a `State` value from its `usize` representation
    fn load(val: usize) -> State {
        State {
            waiting_rx: (val & !3) as *mut WaitingRx,
            sequence: val & 1,
            toggled: (val & 2) == 2,
        }
    }

    /// Return the `usize` representation of this `State`
    fn as_usize(&self) -> usize {
        let mut val = self.waiting_rx as usize | self.sequence;

        if self.toggled {
            val |= 2;
        }

        val
    }

    fn notify_rx(&self) {
        let mut ptr = self.waiting_rx;

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
        if 1 == entry.remaining.fetch_sub(1, AcqRel) {
            // Remove the value
            unsafe { (*entry.value.get()) = None };

            // The next step is to release the slot. This is done by updating
            // the entry state.
            //
            // Load the current state, `Acquire` is used in case a `TX` is
            // waiting
            let mut entry_state = entry.state.load(Acquire);

            loop {
                if entry_state.toggled {
                    // A TX is waiting
                    unimplemented!();
                }

                let next = State {
                    toggled: true,
                    .. entry_state
                };

                let actual = entry.state.compare_and_swap(entry_state, next, Release);

                if entry_state == actual {
                    break;
                }

                entry_state = actual;
            }
        }

        // Increment the position
        self.recv.pos += 1;
    }
}
