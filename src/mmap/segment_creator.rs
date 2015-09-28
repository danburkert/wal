use std::sync::mpsc::{Receiver, Sender, SyncSender, channel, sync_channel};
use std::thread;
use std::io::{Error, ErrorKind, Result};

use mmap::Segment;

pub struct SegmentCreator {
    tx: Sender<Segment>,
    rx: Receiver<Segment>,
}

impl SegmentCreator {

}

fn create_loop(rx: Receiver<Segment>, tx: SyncSender<Segment>) {

    while let Ok(op) = rx.recv() {
        match op {
            FlushOp::Flush(complete) => completions.push(complete),
            FlushOp::Reset(handle) => {
                mmap = handle;
                continue;
            }
        }

        while let Ok(op) = rx.try_recv() {
            match op {
                FlushOp::Flush(complete) => completions.push(complete),
                FlushOp::Reset(handle) => {
                    mmap = handle;
                    break;
                }
            }
        }

        debug!("flushing {} entries", completions.len());

        // TODO: investigate whether doing a ranged flush is more performant.
        let result = mmap.as_mut().flush();
        match result {
            Ok(_) => {
                for complete in completions.drain(..) {
                    complete.complete(())
                }
            }
            Err(error) => {
                warn!("error while flushing mmap: {:?}", error);
                for complete in completions.drain(..) {
                    complete.fail(Error::new(error.kind(), "wal segment flush error"));
                }
                return;
            }
        }
    }
    info!("shutting down");
}
