use std::fs::File;
use std::io::{Error, ErrorKind, Result};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::thread;

use segment::Segment;
use OpenSegment;

pub struct SegmentCreator {
    /// Receive channel for new segments.
    rx: Receiver<OpenSegment>,
    /// The segment creator thread.
    ///
    /// Used for retrieving error upon failure.
    thread: Option<thread::JoinHandle<Result<()>>>
}

impl SegmentCreator {

    /// Creates a new segment creator.
    ///
    /// The segment creator must be started before new segments will be created.
    pub fn new<P>(dir: P, existing: Vec<OpenSegment>) -> SegmentCreator where P: AsRef<Path> {
        let (tx, rx) = sync_channel::<OpenSegment>(3);

        let dir = dir.as_ref().to_path_buf();
        let thread = thread::spawn(move || create_loop(tx, dir, 8192, existing));
        SegmentCreator { rx: rx, thread: Some(thread) }
    }

    /// Retrieves the next segment.
    pub fn next(&mut self) -> Result<OpenSegment> {
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

fn create_loop(tx: SyncSender<OpenSegment>,
               mut path: PathBuf,
               capacity: usize,
               mut existing_segments: Vec<OpenSegment>) -> Result<()> {
    // Ensure the existing segments are in ID order.
    existing_segments.sort_by(|a, b| a.id.cmp(&b.id));

    let mut cont = true;
    let mut id = 0;

    for segment in existing_segments {
        id = segment.id;
        if let Err(_) = tx.send(segment) {
            cont = false;
            break;
        }
    }

    let dir = try!(File::open(&path));

    while cont {
        path.push(format!("open-{}.wal", id));
        let segment = OpenSegment { id: id, segment: try!(Segment::create(&path, capacity)) };
        path.pop();
        id += 1;
        // Sync the directory, guaranteeing that the segment file is durably
        // stored on the filesystem.
        try!(dir.sync_all());

        if let Err(error) = tx.send(segment) {
            cont = false;
        }
    }

    info!("shutting down");
    Ok(())
}

#[cfg(test)]
mod test {

    extern crate tempdir;
    extern crate env_logger;

    use super::SegmentCreator;

    #[test]
    fn test_create_segments() {
        let _ = env_logger::init();
        let dir = tempdir::TempDir::new("segment").unwrap();
        let mut creator = SegmentCreator::new(&dir.path(), vec![]);

        for i in 0..10 {
            let segment = creator.next();
        }
    }
}
