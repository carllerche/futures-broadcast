extern crate futures;
extern crate futures_broadcast as broadcast;

use futures::{Future, Stream, Sink};

use std::time::Duration;
use std::thread;
use std::sync::mpsc;

fn is_send<T: Send>() {}
fn is_sync<T: Sync>() {}

#[test]
fn bounds() {
    is_send::<broadcast::Sender<i32>>();
    is_send::<broadcast::Receiver<i32>>();
    is_sync::<broadcast::Sender<i32>>();
    is_sync::<broadcast::Receiver<i32>>();
}

#[test]
fn send_recv() {
    let (tx, rx) = broadcast::channel::<i32>(16);

    let mut rx1 = rx.new_receiver().wait();
    let mut rx2 = rx.wait();

    let tx = tx.send(1).wait().unwrap();
    let _tx = tx.send(2).wait().unwrap();

    assert_eq!(rx1.next().unwrap(), Ok(1));
    assert_eq!(rx1.next().unwrap(), Ok(2));

    assert_eq!(rx2.next().unwrap(), Ok(1));
    assert_eq!(rx2.next().unwrap(), Ok(2));
}

#[test]
fn receiver_wait() {
    let (mut tx1, rx1) = broadcast::channel::<i32>(1);
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
    let (mut pu, su) = broadcast::channel::<i32>(2);

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

    let (mut pu, su) = broadcast::channel::<i32>(2);

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
        pu = pu.send(i).wait().unwrap();
    }

    for i in 0..N {
        assert_eq!(i, rx.recv().unwrap());
    }

    assert!(rx.recv().is_err());
}

#[test]
fn rx_drops_without_consuming_all() {
    const N: i32 = 25;

    for _ in 0..N {
        thread::sleep(Duration::from_millis(100));
        let (mut pu, su) = broadcast::channel::<i32>(8);

        // Slow RX
        thread::spawn(move || {
            let mut su = su.wait();
            let _ = su.next();

            thread::sleep(Duration::from_millis(7));
            drop(su);
        });

        let mut err = false;

        for i in 0..16 {
            match pu.send(i).wait() {
                Ok(p) => pu = p,
                Err(_) => {
                    err = true;
                    break;
                }
            }
        }

        assert!(err);
    }
}

#[test]
fn test_publisher_closed_immediately() {
    let (_, su) = broadcast::channel::<i32>(16);

    let mut su = su.wait();
    assert!(su.next().is_none());
}
