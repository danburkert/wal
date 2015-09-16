use std::collections::VecDeque;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::thread;
use std::io::{Error, ErrorKind};

use eventual::{Complete, Future};

use mmap::Map;

pub struct Flusher {
    tx: Sender<(usize, Complete<(), Error>)>,
}

impl Flusher {

    pub fn with_offset(map: Map, initial_offset: usize) -> Flusher {
        let (tx, rx) = channel::<(usize, Complete<(), Error>)>();
        thread::spawn(move || flush_loop(map, initial_offset, rx));
        Flusher { tx: tx }
    }

    pub fn flush(&self, offset: usize) -> Future<(), Error> {
        let (complete, future) = Future::pair();
        self.tx
            .send((offset, complete))
            .map(|_| future)
            .unwrap_or(Future::error(Error::new(ErrorKind::Other, "wal segment flusher stopped")))
    }
}

fn flush_loop(mut map: Map,
              initial_offset: usize,
              rx: Receiver<(usize, Complete<(), Error>)>) {
    let mut completions = VecDeque::new();
    let mut offset = initial_offset;

    while let Ok((o, c)) = rx.recv() {
        let mut next_offset = o;
        completions.push_back(c);
        while let Ok((o, c)) = rx.try_recv() {
            next_offset = o;
            completions.push_back(c);
        }

        let mut mmap = map.get_mut();

        trace!("flushing: coalescing {} calls; segment range [{}, {}]",
                completions.len(), offset, next_offset);
        // TODO: investigate whether doing ranged flush is more performant.
        let result = mmap.flush();
        offset = next_offset;

        match result {
            Ok(_) => {
                for complete in completions.drain() {
                    complete.complete(())
                }
            }
            Err(error) => {
                warn!("error while flushing mmap: {:?}", error);
                for complete in completions.drain() {
                    complete.fail(Error::new(error.kind(), "wal segment flush error"));
                }
                return;
            }
        }
    }
    info!("shutting down");
}

#[cfg(test)]
mod test {
}
