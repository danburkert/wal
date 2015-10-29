#![feature(drain, slice_patterns)]
#![cfg_attr(test, feature(custom_attribute, plugin))]
#![cfg_attr(test, plugin(quickcheck_macros))]

extern crate byteorder;
extern crate crc;
extern crate eventual;
extern crate fs2;
extern crate memmap;
extern crate rand;
extern crate time;
#[macro_use]
extern crate log;

mod mmap;
mod segment;

use std::cmp::Ordering;
use std::fmt;
use std::fs::{self, File};
use std::io::{Error, ErrorKind, Result};
use std::mem;
use std::ops;
use std::path::Path;
use std::str::FromStr;
use std::thread;

use eventual::{Complete, Future};
use fs2::FileExt;

use segment::creator::SegmentCreator;
use segment::flusher::SegmentFlusher;
pub use segment::{Segment, SyncSegment};

#[derive(Debug)]
pub struct WalOptions {
    /// The segment capacity. Defaults to 32MiB.
    pub segment_capacity: usize,

    /// The number of segments to create ahead of time, so that appends never
    /// need to wait on creating a new segment.
    pub segment_queue_len: usize,
}

impl Default for WalOptions {
    fn default() -> WalOptions {
        WalOptions {
            segment_capacity: 32 * 1024 * 1024,
            segment_queue_len: 0,
        }
    }
}

/// An open segment and its ID.
///
/// TODO: this shouldn't be public
pub struct OpenSegment {
    pub id: u64,
    pub segment: SyncSegment,
}

/// A closed segment, and the associated start and stop indices.
struct ClosedSegment {
    pub start_index: u64,
    pub end_index: u64,
    pub segment: SyncSegment,
}

enum WalSegment {
    Open(OpenSegment),
    Closed(ClosedSegment),
}

pub struct Wal {
    open_segment: OpenSegment,
    closed_segments: Vec<ClosedSegment>,
    creator: SegmentCreator,
    flusher: SegmentFlusher,
    dir: File,
}

impl Wal {
    pub fn open<P>(path: P) -> Result<Wal> where P: AsRef<Path> {
        Wal::with_options(path, &WalOptions::default())
    }

    pub fn with_options<P>(path: P, options: &WalOptions) -> Result<Wal> where P: AsRef<Path> {
        let dir = try!(File::open(&path));
        try!(dir.try_lock_exclusive());

        // Holds open segments in the directory.
        let mut open_segments: Vec<OpenSegment> = Vec::new();
        let mut closed_segments: Vec<ClosedSegment> = Vec::new();

        for entry in try!(fs::read_dir(&path)) {
            match try!(open_dir_entry(try!(entry))) {
                WalSegment::Open(open_segment) => open_segments.push(open_segment),
                WalSegment::Closed(closed_segment) => closed_segments.push(closed_segment),
            }
        }

        // Validate the closed segments. They must be non-overlapping, and contiguous.
        closed_segments.sort_by(|&ClosedSegment { start_index: left_start, end_index: left_end, .. },
                                 &ClosedSegment { start_index: right_start, end_index: right_end, .. }| {
            (left_start, left_end).cmp(&(right_start, right_end))
        });
        let mut prev_end = None;
        for &ClosedSegment{ start_index, end_index, .. } in &closed_segments {
            if let Some(prev_end) = prev_end {
                if prev_end + 1 != start_index {
                    return Err(Error::new(ErrorKind::InvalidData,
                                          format!("missing segment(s) containing wal
                                                   entries {} to {}", prev_end, start_index)));
                }
            }
            prev_end = Some(end_index)
        }

        // Validate the open segments.
        open_segments.sort_by(|&OpenSegment { id: left_id, .. },
                               &OpenSegment { id: ref right_id, .. }| {
            left_id.cmp(right_id)
        });

        // The latest open segment, may already have segments.
        let mut open_segment: Option<OpenSegment> = None;
        // Unused open segments.
        let mut unused_segments: Vec<OpenSegment> = Vec::new();

        for segment in open_segments {
            if segment.segment.len() > 0 {
                // This segment has already been written to. If a previous open
                // segment has also already been written to, we close it out and
                // replace it with this new one. This may happen because when a
                // segment is closed it is renamed, but the directory is not
                // sync'd, so the operation is not guaranteed to be durable.
                let stranded_segment = open_segment.take();
                open_segment = Some(segment);
                if let Some(segment) = stranded_segment {
                    let closed_segment = try!(close_segment(segment,
                                                            prev_end.map(|i| i + 1).unwrap_or(0)));
                    prev_end = Some(closed_segment.end_index);
                    closed_segments.push(closed_segment);
                }
            } else if open_segment.is_none() {
                open_segment = Some(segment);
            } else {
                unused_segments.push(segment);
            }
        }

        let mut creator = SegmentCreator::new(&path,
                                              unused_segments,
                                              options.segment_capacity,
                                              options.segment_queue_len);

        let open_segment = match open_segment {
            Some(segment) => segment,
            None => try!(creator.next()),
        };

        let flusher = SegmentFlusher::new(open_segment.segment.mmap());

        Ok(Wal {
            open_segment: open_segment,
            closed_segments: closed_segments,
            creator: creator,
            flusher: flusher,
            dir: dir,
        })
    }

    fn retire_open_segment(&mut self) -> Result<()> {
        // TODO: time the next call
        let mut segment = try!(self.creator.next());
        mem::swap(&mut self.open_segment, &mut segment);
        let len = self.closed_segments.len();
        let start_index = if len > 0 { self.closed_segments[len - 1].end_index + 1 } else { 0 };
        try!(self.flusher.reset(segment.segment.mmap()));
        self.closed_segments.push(try!(close_segment(segment, start_index)));
        Ok(())
    }

    pub fn append<T>(&mut self, entry: &T) -> Future<(), Error> where T: ops::Deref<Target=[u8]> {
        if entry.len() > self.open_segment.segment.remaining_size() {
            if let Err(error) = self.retire_open_segment() {
                return Future::error(error);
            }
        }

        // TODO: figure out a real answer for entries bigger the segment size.
        self.open_segment.segment.append(entry).unwrap();
        self.flusher.flush()
    }

    pub fn entry(&self, index: u64) -> Option<&[u8]> {
        let open_start_index = self.closed_segments
                                   .last()
                                   .map(|closed_segment| closed_segment.end_index as u64 + 1)
                                   .unwrap_or(0);
        if index >= open_start_index {
            return self.open_segment.segment.entry((index - open_start_index) as usize);
        }

        match self.closed_segments.binary_search_by(|closed_segment| {
            if index >= closed_segment.start_index {
                if index <= closed_segment.end_index {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            } else {
                Ordering::Less
            }
        }) {
            Ok(segment_index) => {
                let segment = &self.closed_segments[segment_index];
                segment.segment.entry((index - segment.start_index) as usize)
            },
            Err(..) => {
                assert!(index < self.closed_segments
                                    .first()
                                    .map(|closed_segment| closed_segment.start_index)
                                    .unwrap_or(0));
                None
            }
        }
    }
}

impl fmt::Debug for Wal {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Wal {{ segment_count: {} }}",
               self.closed_segments.len() + 1)
    }
}

fn close_segment(OpenSegment { mut segment, id }: OpenSegment,
                 start_index: u64)
                 -> Result<ClosedSegment> {
    let end_index = start_index + segment.len() as u64;

    let new_path = segment.path()
                          .with_file_name(format!("closed-{}-{}", start_index, end_index));
    try!(segment.rename(new_path));
    debug!("closing open segment {} with entries {} through {}", id, start_index, end_index);
    Ok(ClosedSegment { start_index: start_index,
                       end_index: end_index,
                       segment: segment })
}

fn open_dir_entry(entry: fs::DirEntry) -> Result<WalSegment> {
    let metadata = try!(entry.metadata());

    let error = || {
        Error::new(ErrorKind::InvalidData,
                   format!("unexpected entry in wal directory: {:?}", entry.path()))
    };

    if !metadata.is_file() {
        return Err(error());
    }

    let filename = try!(entry.file_name().into_string().map_err(|_| error()));
    match &*filename.split('-').collect::<Vec<&str>>() {
        ["open", id] => {
            let id = try!(u64::from_str(id).map_err(|_| error()));
            let segment = try!(SyncSegment::open(entry.path()));
            Ok(WalSegment::Open(OpenSegment { segment: segment, id: id }))
        },
        ["closed", start, end] => {
            let start = try!(u64::from_str(start).map_err(|_| error()));
            let end = try!(u64::from_str(end).map_err(|_| error()));
            let segment = try!(SyncSegment::open(entry.path()));
            Ok(WalSegment::Closed(ClosedSegment { start_index: start,
                                                  end_index: end,
                                                  segment: segment }))
        },
        _ => Err(error()),
    }
}

#[cfg(test)]
mod test {
    extern crate tempdir;
    extern crate env_logger;
    extern crate quickcheck;

    use std::error::Error;

    use eventual::{Async, Future, Join};
    use fs2;
    use test::quickcheck::TestResult;

    use super::Wal;

    /// Check that entries appended to the write ahead log can be read back.
    #[test]
    fn check_wal() {
        let _ = env_logger::init();
        fn wal(entries: Vec<Vec<u8>>) -> TestResult {
            let dir = tempdir::TempDir::new("wal").unwrap();
            let mut wal = Wal::open(&dir.path()).unwrap();

            for entry in &entries {
                if let Err(error) = wal.append(entry).await() {
                    return TestResult::error(error.description());
                }
            }

            for (index, expected) in entries.iter().enumerate() {
                match wal.entry(index as u64) {
                    Some(entry) if entry != &expected[..] => return TestResult::failed(),
                    None => return TestResult::failed(),
                    _ => (),
                }
            }
            TestResult::passed()
        }

        quickcheck::quickcheck(wal as fn(Vec<Vec<u8>>) -> TestResult);
    }

    /// Check that the Wal will read previously written entries.
    #[test]
    fn check_wal_reopen() {
        let _ = env_logger::init();
        fn wal(entries: Vec<Vec<u8>>) -> TestResult {
            let dir = tempdir::TempDir::new("wal").unwrap();
            {
                let mut wal = Wal::open(&dir.path()).unwrap();
                for entry in &entries {
                    let _ = wal.append(entry);
                }
            }

            let wal = Wal::open(&dir.path()).unwrap();
            // Check that all of the entries are present.
            for (index, expected) in entries.iter().enumerate() {
                match wal.entry(index as u64) {
                    Some(entry) if entry != &expected[..] => return TestResult::failed(),
                    None => return TestResult::failed(),
                    _ => (),
                }
            }
            TestResult::passed()
        }

        quickcheck::quickcheck(wal as fn(Vec<Vec<u8>>) -> TestResult);
    }

    #[test]
    fn test_append() {
        let _ = env_logger::init();
        let dir = tempdir::TempDir::new("wal").unwrap();
        let mut wal = Wal::open(&dir.path()).unwrap();

        let entry: &[u8] = &[42u8; 4096];
        let mut completions = Vec::new();

        for _ in 1..10 {
            completions.push(wal.append(&entry));
        }

        let (c, f) = Future::pair();
        completions.join(c);
        f.await().unwrap();
    }

    /// Tests that two Wal instances can not coexist for the same directory.
    #[test]
    fn test_exclusive_lock() {
        let _ = env_logger::init();
        let dir = tempdir::TempDir::new("wal").unwrap();
        let wal = Wal::open(&dir.path()).unwrap();
        assert_eq!(fs2::lock_contended_error().kind(),
                   Wal::open(&dir.path()).unwrap_err().kind());
        drop(wal);
        assert!(Wal::open(&dir.path()).is_ok());
    }
}
