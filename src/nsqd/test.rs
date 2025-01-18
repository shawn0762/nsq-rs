use std::{
    cell::RefCell,
    sync::{atomic::AtomicU16, Arc, Mutex},
};

pub struct A {
    name: String,

    counter: AtomicU16,

    state: Mutex<State>,
}

impl A {
    pub fn new(name: String, rx: async_channel::Receiver<i16>) -> Self {
        let state = Mutex::new(State {
            counter: 0.into(),
            rx,
        });
        let counter = 0.into();
        Self {
            name,
            counter,
            state,
        }
    }

    pub fn incre(self: &Arc<Self>) {
        self.state
            .lock()
            .unwrap()
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
    }

    pub fn serve(self: &Arc<Self>) {
        loop {
            self.state.lock().unwrap().rx.recv();
        }
    }
}

struct State {
    counter: AtomicU16,

    rx: async_channel::Receiver<i16>,
}

mod test {
    use std::{
        sync::Arc,
        thread::{self, spawn, Thread},
        time::Duration,
    };

    use super::A;

    #[test]
    fn test_a() {
        let (tx, rx) = async_channel::bounded(10);
        let a = Arc::new(A::new("name".to_owned(), rx));
        let b = a.clone();
        let h1 = spawn(move || {
            b.serve();
        });

        let c = a.clone();
        let h2 = spawn(move || {
            thread::sleep(Duration::from_secs(5));
            c.counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        });

        h1.join();
        h2.join();
    }
}
