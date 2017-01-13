extern crate futures;
extern crate futures_pubsub as pubsub;

use futures::{Future, Stream, Sink, Async, AsyncSink};
use futures::future::lazy;

use std::time::Duration;
use std::thread;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};

fn is_send<T: Send>() {}
fn is_sync<T: Sync>() {}

#[test]
fn bounds() {
    is_send::<pubsub::Sender<i32>>();
    is_send::<pubsub::Receiver<i32>>();
    is_sync::<pubsub::Sender<i32>>();
    is_sync::<pubsub::Receiver<i32>>();
}

#[test]
fn smoke() {
// fn send_recv() {
    let (tx, rx) = pubsub::channel::<i32>(16);

    let mut rx1 = rx.new_receiver().wait();
    let mut rx2 = rx.wait();

    let tx = tx.send(1).wait().unwrap();
    let tx = tx.send(2).wait().unwrap();

    assert_eq!(rx1.next().unwrap(), Ok(1));
    assert_eq!(rx1.next().unwrap(), Ok(2));

    assert_eq!(rx2.next().unwrap(), Ok(1));
    assert_eq!(rx2.next().unwrap(), Ok(2));
}
