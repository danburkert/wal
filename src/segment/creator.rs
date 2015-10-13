use std::fs::File;
use std::io::{Error, ErrorKind, Result};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::thread;

use mmap::Segment;

pub struct SegmentCreator {
    /// Receive channel for new segments.
    rx: Receiver<Segment>,
    /// The segment creator thread.
    ///
    /// Used for retrieving error upon failure.
    thread: Option<thread::JoinHandle<Result<()>>>
}

impl SegmentCreator {

    /// Creates a new segment creator.
    ///
    /// The segment creator must be started before new segments will be created.
    pub fn new<P>(dir: P, id: u32) -> SegmentCreator where P: AsRef<Path> {
        let (tx, rx) = sync_channel::<Segment>(3);

        let dir = dir.as_ref().to_path_buf();
        let thread = thread::spawn(move || create_loop(tx, dir, 8192, id));
        SegmentCreator { rx: rx, thread: Some(thread) }
    }

    /// Retrieves the next segment.
    pub fn next(&mut self) -> Result<Segment> {
        self.rx.recv().map_err(|_| {
            match self.thread.take().map(|join_handle| join_handle.join()) {
                Some(Ok(Err(error))) => error,
                None => Error::new(ErrorKind::Other, "segment creator thread already failed"),
                Some(Ok(Ok(()))) => unreachable!("segment creator thread finished without an error,
                                                  but the segment creator is still live"),
                Some(Err(_)) => unreachable!("segment creator thread panicked"),
            }
        })
    }
}

fn create_loop(tx: SyncSender<Segment>,
                mut path: PathBuf,
                capacity: usize,
                mut id: u32) -> Result<()> {
    loop {
        path.push(format!("open-{}.wal", id));
        let segment = try!(Segment::create(&path, capacity));
        path.pop();
        id += 1;

        if let Err(error) = tx.send(segment) {
            break;
        }
    }

    info!("shutting down");
    Ok(())
}

#[cfg(test)]
mod test {

    #[test]
    fn test_create_segments() {
        let _ = env_logger::init();
        let dir = tempdir::TempDir::new("segment").unwrap();
        let creator = SegmentCreator::new(dir, 0);

        for i in 0..10 {
            let segment = creator.next();
        }
    }
}
