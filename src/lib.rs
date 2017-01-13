//! A multi-producer, multi-consumer, futures-aware broadcast channel with back
//! pressure.
//!
//! A broadcast channel can be used as a communication primitive between tasks
//! running on `futures-rs` executors. Unlike normal channels, each broadcast
//! channel `Receiver` will receive every value sent by `Sender` handles.
//!
//! Broadcast channels are backed by a pre-allocated vector of slots. The
//! capacity is specified at creation.
//!
//! # Producers
//!
//! `Sender` implements `Sink` and allows a task to send values to all
//! outstanding consumers. If the underlying value buffer is full due to slow
//! consumers, then the send operation will fail with `NotReady` and the task
//! will be notified once there is additional capacity available.
//!
//! **Note**: A limitation of the broadcast channel is that the capacity
//! specified at creation time also limits the number of outstanding producer
//! handles. In other words, if a broadcast channel is created with a capacity
//! of `16`, then there can only be at most 16 `Sender` handles in existence at
//! the same time.
//!
//! # Consumers
//!
//! `Receiver` implements `Stream` and allows a task to read values out of the
//! channel. `Receiver` also has a `recv` function which returns a reference to
//! the sent value. This allows all consumers to observe the value without
//! requiring cloning.
//!
//! If there is no new value to read from the channel, the task will be notified
//! once the next value is sent.
//!
//! **Note**: Since all consumers observe all values sent, any single consumer
//! could cause the channel's capacity to fill up. As such, users of the
//! broadcast channel must take care to respond swiftly to notifications in
//! order to avoid blocking producers.

// The broadcast channel is based on an MPMC array queue. It is similar to the
// bounded MPMC queue described by 1024cores [1]. However, it differens in that
// all consumers must see all values. This is done by having each entry in the
// buffer track the number of remaining consumers  When a value is pushed into
// the channel, the current number of outstanding consumers is used to
// initialize the "remaining consumers" value. Whenever a consumer sees the
// value, this number is decremented. When it reaches zero, the entry is freed
// and made available again to the producers.
//
// Similar to the dynamic mpsc channel [2], each sender gets one "guaranteed"
// slot in order to hold a message. This allows `Sender` to know for a fact that
// a send will succeed *before* starting to do the actual work. If this
// dedicates lot is in use, then the `Sender` is unable to send a value and
// `NotReady` will be returned on a send attempt.
//
// The channel is implemented without using locks. Each entry manages its own
// state with a single atomic variable. A single atomic variable is used to
// manage the number of outstanding receiver handles as well as the index of the
// next available entry.
//
// # Operations
//
// The steps for sending a value are roughly:
//
// 1) Claim a slot by fetching and incrementing the next available entry
//    variable. This also returns the number of outstanding receive handles.
// 2) If the entry is occupied, use the `WaitingTx` node to hold the value until
//    the entry becomes available.
// 3) Store the value in the entry
// 4) Toggle the entry state to indicate that a value is present
// 5) Notify any waiting consumers.
//
// The steps for receiving a value are roughly:
//
// 1) Read the state of the entry at the receivers's current position.
// 2) If there is no value, wait for one to become available.
// 3) Observe the value (via a reference to the value in the buffer).
// 4) Decrement the atomic tracking the number of remaining consumers to observe
//    the value.
// 5) If this is the last consumer, continue, otherwise return.
// 6) Drop the value from the buffer
// 8) If a producer is waiting for the slot, immediately let it place the value
//    stored in it's waiter node into the newly available entry. The entry is
//    now in use with the value.
// 7) If no producer is waiting, then toggle the entry state to indicate that
//    the entry is now available.
//
// The specific details are a bit more complicated and are documented inline
//
// # Entry state
//
// Each entry maintains an atomic variable which is used to track the state of
// the entry and used to handle memory ordering. The data tracked by the entry
// state consists of:
//
// * Pointer to the next consumer waiter node
// * Sequence value, this is either 0 or 1.
// * Toggle flag, which has a different meaning if it is the consumer or the
//   producer which flips it.
//
// This state is stored in a single AtomicPtr, leveraging the fact that the 2
// least significant bits in a pointer are never used due to pointer alignment.
//
// The sequence value manages the state of the entry. It is similar to the
// `sequence` value used in the 1024cores queue, however it can be limited to 1
// bit. This is acceptable due to the fact that consumers track their own
// positions (vs using an atomic in the 1024cores queue) and producers cannot
// claim an entry until all consumers have "released" that entry, which prevents
// race conditions where producers "lap" consumers.
//
// In the case of slow consumers, it is possible for both a producer and
// multiple consumers to wait on the slot. The producer gets blocked on the slow
// consumer and fast consumers are blocked waiting for the producer to send a
// value. In such a case, the fast consumer will encounter an entry that has a
// value and must be able to determine that it is a value that it has already
// seen and not a new one. This is handled by the `sequence` component of the
// entry state.
//
// The sequence bit is essentially `(position / channel-capacity) & 1`, where
// position is a monotonically increasing integer. Aka, it is the least
// significant bit representing the number of times a sender or receiver has
// cycled the buffer. Given that it is impossible for handles to lap each other
// more than once, this is sufficient amount of state to disambiguate the above
// scenario.
//
// The `toggle` flag implies different things depending on if the sender or
// receiver set it. When a sender encounters an occupied entry, it will store
// its `WaitingTx` node in the entry's `waiting_tx` variable. It will then
// attempt to flip `toggle` from `false` to `true. If the compare-and-swap
// operation is successful, then the sender has entered the waiting state, if
// the compare-and-swap fails, then the entry is available to the sender. When a
// receiver is done with a value, it will release the entry by flipping `toggle`
// from `false` to `true. If the compare-and-swap fails, then a sender is
// waiting on the entry, and the receiver must unpark it.
//
// # References
//
// [1] http://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue
// [2] https://github.com/carllerche/futures-mpsc

#[macro_use]
extern crate futures;

mod atomic_task;
mod atomic_u64;

pub use std::sync::mpsc::SendError;

use atomic_task::AtomicTask;
use atomic_u64::AtomicU64;

use futures::{Stream, Sink, Poll, StartSend, Async, AsyncSink};
use futures::task;

use std::{ops, mem, ptr, u32, usize};
use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, AtomicPtr};
use std::sync::atomic::Ordering::{self, Acquire, Release, AcqRel, Relaxed};

/// The broadcast end of a channel which is used to send values.
///
/// This is created by the `channel` function.
pub struct Sender<T> {
    // Channel state shared between the sender and receiver
    inner: Arc<Inner<T>>,

    // Handle to the waiter node associated with this `Sender`. This is lazily
    // allocated, so if the `Sender` never blocks, the node is never allocated.
    // Once it is allocated, it is reused for the lifetime of the `Sender`
    // handle.
    //
    // The handle holds the waiting task as well as the sent value that caused
    // the sender to block.
    waiter: Option<Arc<WaitingTx<T>>>,
}

/// The receiving end of a channel.
///
/// Each `Receiver` will receive every value sent by every `Sender`.
///
/// This is created by the `channel` function.
pub struct Receiver<T> {
    // Channel state shared between the sender and receiver
    inner: Arc<Inner<T>>,

    // The current receiver's position. Usually, when the `Receiver` is created,
    // this is initialized to the head of the channel. After the receiver reads
    // a value, `pos` is incremented.
    pos: usize,

    // Handle to the waiter node associated with this `Receiver`. This is lazily
    // allocated, so if the `Receiver` never blocks, the node is never
    // allocated. Once it is allocated, it is reused for the lifetime of the
    // `Receiver` handle.
    //
    // The handle holds the waiting task.
    waiter: Option<Arc<WaitingRx>>,
}

/// A reference to the received value still located in the channel buffer.
///
/// When the `RecvGuard` is dropped, the value will be released. This type
/// derefs to the value.
pub struct RecvGuard<'a, T: 'a> {
    // Reference to the receiver handle. This is a mutable reference to ensure
    // exclusive access to the receiver.
    recv: &'a mut Receiver<T>,

    // Reference to the value
    value: &'a T,
}

struct Inner<T> {
    // Pre-allocated buffer of entries
    buffer: Vec<Entry<T>>,

    // Buffer access mask This is `capacity - 1` and allows mapping a position
    // in the channel to an index.
    mask: usize,

    // Sequence value mask
    seq_mask: usize,

    // Used by publishers and when receivers are cloned.
    //
    // This is an atomic variable containing the number of outstanding receiver
    // handles as well as the entry position for the next value sent.
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

    // Number of remaining receivers to observe the value.
    //
    // This value can actually go "negative" (though it will wrap) in some racy
    // cases.
    remaining: AtomicUsize,

    // Pointer to a waiting TX node.
    waiting_tx: UnsafeCell<Option<Arc<WaitingTx<T>>>>,
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

    // Queued (value, num-rx)
    value: UnsafeCell<Option<(Option<T>, usize)>>,

    // True if parked
    parked: AtomicUsize,
}

const READY: usize = 0;
const PARKED: usize = 1;
const CLOSED: usize = 2;

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
    /// Returns `Ready` if the channel currently has capacity
    pub fn poll_ready(&mut self) -> Async<()> {
        if self.is_parked() {
            Async::NotReady
        } else {
            Async::Ready(())
        }
    }

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
            Some(ref w) => w.parked.load(Acquire) == PARKED,
            None => false,
        }
    }
}

impl<T> Sink for Sender<T> {
    type SinkItem = T;
    type SinkError = SendError<T>;

    fn start_send(&mut self, item: T) -> StartSend<T, SendError<T>> {
        if self.is_parked() {
            return Ok(AsyncSink::NotReady(item));
        }

        match self.inner.send(Some(item), &mut self.waiter, true) {
            Err(SendError(e)) => Err(SendError(e.unwrap())),
            Ok(_) => Ok(AsyncSink::Ready),
        }
    }

    fn poll_complete(&mut self) -> Poll<(), SendError<T>> {
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

fn tx_waiter<T>(cell: &mut Option<Arc<WaitingTx<T>>>,
                item: Option<T>,
                num_rx: usize,
                park: bool) -> Arc<WaitingTx<T>>
{
    if let Some(ref w) = *cell {
        unsafe {
            if park {
                w.task.park();
            }

            (*w.value.get()) = Some((item, num_rx));
        }

        w.parked.store(PARKED, Release);

        return w.clone();
    }

    let w = Arc::new(WaitingTx {
        task: AtomicTask::new(task::park()),
        value: UnsafeCell::new(Some((item, num_rx))),
        parked: AtomicUsize::new(PARKED),
    });

    *cell = Some(w.clone());
    w
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let prev = self.inner.num_tx.fetch_sub(1, AcqRel);

        if prev == 1 {
            if let Some(ref w) = self.waiter {
                match w.parked.compare_and_swap(PARKED, CLOSED, AcqRel) {
                    PARKED | CLOSED => return,
                    _ => {}
                }
            }

            // Send a "closed" message
            let _ = self.inner.send(None, &mut self.waiter, false);
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
                    if seq == actual.sequence {
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
                match *entry.value.get() {
                    Some(ref v) => v,
                    None => {
                        return Ok(Async::Ready(None));
                    }
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

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let pos = self.inner.pub_state.dec_rx();
        let mask = self.inner.mask;

        while self.pos < pos {
            let entry = &self.inner.buffer[self.pos & mask];

            // Decrement the receiver
            if 1 == entry.remaining.fetch_sub(1, AcqRel) {
                // Last remaining receiver, release the entry
                entry.release(&self.inner);
            }

            self.pos += 1;
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
        //
        // Why is this AcqRel?
        let prev = entry.remaining.fetch_sub(1, AcqRel);

        if 1 == prev {
            entry.release(&self.recv.inner);
        }

        // Increment the position
        self.recv.pos += 1;
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

        for _ in 0..capacity {
            buffer.push(Entry {
                state: StateCell::new(),
                value: UnsafeCell::new(None),
                remaining: AtomicUsize::new(0),
                waiting_tx: UnsafeCell::new(None),
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


    fn send(&self, mut item: Option<T>,
            waiter: &mut Option<Arc<WaitingTx<T>>>,
            park: bool)
        -> Result<(), SendError<Option<T>>>
    {
        let mask = self.mask;
        let seq_mask = self.seq_mask;

        // Claim a lot, this does not need to be guarded like in the original
        // MPMC queue because the size restrictions on the channel and the
        // `is_parked` is a sufficient guard.
        let pub_state = match self.pub_state.claim_slot() {
            Some(v) => v,
            None => return Err(SendError(item)),
        };

        // Get the sequence value for the given position
        let seq = pos_to_sequence(pub_state.pos(), seq_mask);

        // Get a handle to the entry
        let entry = &self.buffer[pub_state.pos() & mask];

        // Load the current entry state
        let mut entry_state = entry.state.load(Acquire);

        loop {
            debug_assert!(seq != entry_state.sequence);

            if entry_state.toggled {
                // The subscriber has released this slot and the publisher may
                // write a value to it.
                unsafe {
                    // Set the value
                    (*entry.value.get()) = item;
                }

                // Set the number of remaining subscribers to observe
                //
                // This is done with a `fetch_add` to work concurrently with
                // receivers that are in the process of dropping.
                //
                // The `Relaxed` ordering is sufficient here as all receivers with
                // `Acquire` this memory when loading the entry state.
                let prev = entry.remaining.fetch_add(pub_state.num_rx(), Relaxed);

                if 0 == prev.wrapping_add(pub_state.num_rx()) {
                    // The receiving end is disconnected, so return the value
                    let item = unsafe { (*entry.value.get()).take() };
                    return Err(SendError(item));
                }

                // A CAS loop for updating the entry state.
                loop {
                    let next = State {
                        waiting_rx: ptr::null_mut(),
                        sequence: seq,
                        toggled: false,
                        .. entry_state
                    };

                    let actual = entry.state.compare_and_swap(entry_state, next, AcqRel);

                    if actual == entry_state {
                        break;
                    }

                    entry_state = actual;
                }

                // Notify waiters
                entry_state.notify_rx();

                return Ok(());

            } else {
                // The slot is occupied, so store the item in the waiting_tx
                // slot

                let mut waiter = tx_waiter(waiter, item, pub_state.num_rx(), park);

                // Store the waiter in the slot
                unsafe {
                    // The slot should be empty...
                    debug_assert!((*entry.waiting_tx.get()).is_none());

                    // Store the waiter
                    (*entry.waiting_tx.get()) = Some(waiter);
                };

                let next = State {
                    // Set the toggle, indicating that the TX is waiting
                    toggled: true,
                    .. entry_state
                };

                // Attempt to CAS
                let actual = entry.state.compare_and_swap(entry_state, next, AcqRel);

                if entry_state == actual {
                    return Ok(());
                }

                // The CAS failed, remove the waiting node, re-acquire the item,
                // and try the process again
                waiter = unsafe { (*entry.waiting_tx.get()).take().unwrap() };
                let (i, _) = unsafe { (*waiter.value.get()).take().unwrap() };
                item = i;

                // Clear parked flag
                waiter.parked.store(READY, Relaxed);

                entry_state = actual;
            }
        }
    }
}

#[cfg(debug_assertions)]
impl<T> Drop for Inner<T> {
    fn drop(&mut self) {
        // At this point, since all `Sender` and `Receiver` handles have dropped
        // and cleaned up after themselves, all unsafe pointers are null.
        for entry in &self.buffer {
            let entry_state = entry.state.load(Acquire);

            debug_assert!(entry_state.waiting_rx.is_null());

            unsafe {
                debug_assert!((*entry.waiting_tx.get()).is_none());
            }
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
    /// Returns `None` if there are no more receivers.
    ///
    /// Uses `Relaxed` ordering
    fn claim_slot(&self) -> Option<PubState> {
        let mut curr = self.load(Relaxed);

        loop {
            if curr.num_rx == 0 {
                return None;
            }

            let next = PubState {
                pos: curr.pos.wrapping_add(1),
                .. curr
            };

            let actual = self.compare_and_swap(curr, next, Relaxed);

            if curr == actual {
                return Some(curr);
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

    /// Decrement the number of outstanding receivers
    fn dec_rx(&self) -> usize {
        let mut curr = self.load(Relaxed);

        loop {
            let next = PubState {
                num_rx: curr.num_rx - 1,
                .. curr
            };

            let actual = self.compare_and_swap(curr, next, Relaxed);

            if curr == actual {
                return curr.pos();
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
    fn release(&self, inner: &Inner<T>) {
        // The acquire is done here to ensure that the memory ordering has been
        // established for the entry value.
        let mut entry_state = self.state.load(Acquire);

        // Remove the value
        unsafe { (*self.value.get()) = None };

        loop {
            if entry_state.toggled {
                let waiter = unsafe {
                    // A TX is waiting, take the waiter..
                    let waiter = (*self.waiting_tx.get()).take().unwrap();

                    // Get the item
                    let (item, num_rx) = (*waiter.value.get()).take().unwrap();

                    // Store the item in the entry
                    (*self.value.get()) = item;

                    // Reset the number of remaining receivers to observe
                    // the value.
                    //
                    // A `store` is safe here as it is not possible for another
                    // RX to concurrenly modify the value.
                    //
                    // `Relaxed` ordering is used as the entry state gates
                    // reads.
                    self.remaining.store(num_rx, Relaxed);

                    waiter
                };

                let next = State {
                    waiting_rx: ptr::null_mut(),
                    sequence: (entry_state.sequence + 1) % 2,
                    toggled: false,
                    .. entry_state
                };

                // At this point, this is the only thread that will attempt
                // to mutate the state slot, so there is no need for a CAS.
                let prev = self.state.swap(next, Release);
                debug_assert!(entry_state == prev);

                // Unpark the TX waiter
                match waiter.parked.compare_and_swap(PARKED, READY, AcqRel) {
                    CLOSED => {
                        let mut waiter = Some(waiter);
                        let _ = inner.send(None, &mut waiter, false);
                    }
                    PARKED => {
                        waiter.task.unpark();
                    }
                    _ => unreachable!(),
                }

                // Unpark any RX waiting on the slot
                entry_state.notify_rx();

                break;
            }

            let next = State {
                toggled: true,
                .. entry_state
            };

            let actual = self.state.compare_and_swap(entry_state, next, Release);

            if entry_state == actual {
                break;
            }

            entry_state = actual;
        }
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

    fn swap(&self, next: State, ordering: Ordering) -> State {
        let val = self.state.swap(next.as_usize(), ordering);
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

                ptr = node.next.swap(ptr::null_mut(), Release);

                // Unpark the task
                node.task.unpark();
            }
        }
    }
}
