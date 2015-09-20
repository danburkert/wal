use std::collections::VecDeque;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::thread;
use std::io::{Error, ErrorKind};

use eventual::{Complete, Future};

use mmap::Map;

const PAGE_SIZE: usize = 4096;

pub struct Flusher {
    tx: Sender<(usize, Complete<(), Error>)>,
    thread: JoinHandle<()>,
}

impl Flusher {

    pub fn new(mut map: Map) -> Flusher {

        let (tx, rx) = channel::<(usize, Complete<(), Error>)>();

        thread::spawn(move|| {
            let mut buf = VecDeque::new();
            let mut offset = 0;

            while let Ok((o, c)) = rx.recv() {
                buf.push_back((o, c));
                let mut next_offset = o;
                while let Ok((o, c)) = rx.try_recv() {
                    buf.push_back((o, c));
                    next_offset = o;
                }

                next_offset = round_to_page(next_offset);
                let mut mmap = map.get_mut();

                if next_offset > offset {
                    trace!("flushing mmap range: offset = {}, len = {}",
                           offset, next_offset - offset);
                    let result = unsafe { mmap.flush_range(offset, next_offset - offset) };
                    offset = next_offset;

                    match result {
                        Ok(_) => {
                            while !buf.is_empty() {
                                if buf[0].0 < offset {
                                    buf.pop_front().unwrap().1.complete(());
                                } else {
                                    break;
                                }
                            }
                        }
                        Err(error) => {
                            warn!("error while flushing mmap: {:?}", error);
                            for (_, complete) in buf {
                                complete.fail(Error::new(error.kind(), "wal segment flush error"));
                            }
                            return;
                        }
                    }
                }
            }
        });

        Flusher {
            tx: tx,
        }
    }

    fn flush(&self, offset: usize) -> Future<(), Error> {
        let (complete, future) = Future::pair();
        match self.tx.send((offset, complete)) {
            Ok(_) => future,
            Err(error) => Future::error(Error::new(ErrorKind::Other,
                                                   "wal segment flusher stopped")),
        }
    }
}

impl drop for Flusher {
    pub fn drop(&mut self) {
        // TODO: this will likely deadlock the thread when it is dropped; the channel probably
        // needs to be closed as well.
        let _ = self.thread.join();
    }
}

fn round_to_page(len: usize) -> usize {
    len - (len % PAGE_SIZE)
}

#[cfg(test)]
mod test {

    use super::round_to_page;

    #[test]
    fn test_round_to_page() {
        assert_eq!(4096, round_to_page(4096));
        assert_eq!(4096, round_to_page(5000));
        assert_eq!(4096, round_to_page(8191));
        assert_eq!(8192, round_to_page(8192));
        assert_eq!(8192, round_to_page(8192));
        assert_eq!(8192, round_to_page(8193));
        assert_eq!(16384, round_to_page(16483));
    }
}
