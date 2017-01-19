extern crate futures;
extern crate futures_pubsub as pubsub;

use futures::{Future, Stream, Sink, Async, AsyncSink};
use futures::future::lazy;

use std::time::Duration;
use std::thread;
use std::sync::{mpsc, Arc, Mutex};
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
fn send_recv() {
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

#[test]
fn receiver_wait() {
    let (mut tx1, rx1) = pubsub::channel::<i32>(1);
    let (tx2, rx2) = mpsc::channel();

    {
        thread::spawn(move || {
            let mut rx = rx1.wait();

            for _ in 0..5 {
                tx2.send(rx.next().unwrap().unwrap()).unwrap();
            }
        });
    }

    for i in 0..5 {
        thread::sleep(Duration::from_millis(50));

        tx1 = tx1.send(i).wait().unwrap();
        assert_eq!(rx2.recv().unwrap(), i);
    }

    drop(tx1);
}

#[test]
fn wrapping_receiver_wait() {
    let (mut pu, su) = pubsub::channel::<i32>(2);

    let (tx1, rx1) = mpsc::channel();
    let (tx2, rx2) = mpsc::channel();

    // Fast subscriber
    {
        let su = su.new_receiver();

        thread::spawn(move || {
            let mut su = su.wait();

            for _ in 0..4 {
                tx1.send(su.next().unwrap().unwrap()).unwrap();
            }
        });
    }

    // Slow subscriber
    {
        thread::spawn(move || {
            let mut su = su.wait();

            for _ in 0..4 {
                thread::sleep(Duration::from_millis(50));
                tx2.send(su.next().unwrap().unwrap()).unwrap();
            }
        });
    }

    for i in 0..2 {
        for j in 0..2 {
            thread::sleep(Duration::from_millis(10));

            pu = pu.send(i * 2 + j).wait().unwrap();
        }

        for rx in &[&rx1, &rx2] {
            for j in 0..2 {
                assert_eq!(i * 2 + j, rx.recv().unwrap());
            }
        }
    }

    for rx in &[rx1, rx2] {
        assert!(rx.recv().is_err());
    }

    drop(pu);
}

#[test]
fn single_tx_wait() {
    const N: i32 = 25;

    let (mut pu, su) = pubsub::channel::<i32>(2);

    let (tx, rx) = mpsc::channel();

    // Slow subscriber
    thread::spawn(move || {
        let mut su = su.wait();

        for _ in 0..N {
            thread::sleep(Duration::from_millis(10));
            tx.send(su.next().unwrap().unwrap()).unwrap();
        }
    });

    // Fast producer
    for i in 0..N {
        println!("SEND {:?}", i);
        pu = pu.send(i).wait().unwrap();
    }

    for i in 0..N {
        assert_eq!(i, rx.recv().unwrap());
    }

    assert!(rx.recv().is_err());
}
