mod flusher;
mod segment;
mod segment_creator;

use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::io::{Error, ErrorKind, Result};
use std::str::FromStr;

use memmap::Mmap;

pub use self::segment::Segment;
pub use self::flusher::Flusher;

/// A cloneable handle to a memory map.
///
/// Allowing mutable access to a memory map is, in general, completely unsafe.
/// The caller must ensure that the memory map is only mutable accessed from a
/// single thread at once.
///
/// `Segment` and `Flusher` take advantage of `MmapHandle` to write to the map
/// from the main thread, and flush changes asynchronously in a background
/// thread.
#[derive(Clone)]
pub struct MmapHandle {
    map: Arc<UnsafeCell<Mmap>>,
}

impl MmapHandle {
    /// Creates a new cloneable memory map from the memory map.
    pub fn new(map: Mmap) -> MmapHandle {
        MmapHandle {
            map: Arc::new(UnsafeCell::new(map)),
        }
    }
}

impl AsRef<Mmap> for MmapHandle {
    fn as_ref(&self) -> &Mmap {
        unsafe { &*self.map.get() }
    }
}

impl AsMut<Mmap> for MmapHandle {
    fn as_mut(&mut self) -> &mut Mmap {
        unsafe { &mut *self.map.get() }
    }
}

unsafe impl Send for MmapHandle { }

pub struct Wal {
    open_segment: (usize, Segment),
    closed_segments: VecDeque<(usize, Segment)>,
    flusher: Flusher,
}

impl Wal {
    pub fn new<P>(path: P) -> Result<Wal> where P: AsRef<Path> {
        let mut open_segment: Option<(usize, Segment)> = None;
        let mut closed_segments: Vec<(usize, usize, Segment)> = Vec::new();
        let mut new_segments: Vec<Segment> = Vec::new();

        for entry in try!(fs::read_dir(path)) {
            match try!(open_dir_entry(try!(entry))) {
                WalSegment::OpenSegment(segment, start) => {
                    if open_segment.is_none() {
                        open_segment = Some((start, segment))
                    } else {
                        return Err(Error::new(ErrorKind::InvalidData,
                                              "multiple open wal segments found"));
                    }
                },
                WalSegment::ClosedSegment(segment, start, end) => {
                    closed_segments.push((start, end, segment));
                },
                WalSegment::NewSegment(segment) => {
                    new_segments.push(segment);
                },
            }
        }

        // Validate the closed segments. They must be non-overlapping, and
        // contiguous.
        closed_segments.sort_by(|&(left_start, left_end, _), &(right_start, right_end, _)| {
            (left_start, left_end).cmp(&(right_start, right_end))
        });
        let mut prev_end = None;
        for &(start, end, _) in &closed_segments {
            if let Some(prev_end) = prev_end {
                if prev_end + 1 != start {
                    return Err(Error::new(ErrorKind::InvalidData,
                                          format!("missing segment(s) containing wal entries {} to {}",
                                                  prev_end, start)));
                }
            }
            prev_end = Some(end)
        }

        // The open segment must start where the closed segments leave off.
        if let Some(prev_end) = prev_end {
            if let Some((start, _)) = open_segment {
                if prev_end + 1 != start {
                    return Err(Error::new(ErrorKind::InvalidData,
                                          format!("missing segment(s) containing wal entries {} to {}",
                                                  prev_end, start)));
                }
            }
        }

        /*
        Wal {
            open_segment = open_segment,
            closed_segments = closed_segments.into_iter()
                                             .map(|(start, _, segment)| (start, segment))
                                             .collect();


        }
        */


        unimplemented!()
    }
}

enum WalSegment {
    /// An open segment, and the associated start index.
    OpenSegment(Segment, usize),

    /// A closed segment, and the associated start and stop indices.
    ClosedSegment(Segment, usize, usize),

    /// A new segment.
    NewSegment(Segment),
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
        ["wal", "segment", "open", start] => {
            let start = try!(usize::from_str(start).map_err(|_| error()));
            let segment = try!(Segment::open(entry.path()));
            Ok(WalSegment::OpenSegment(segment, start))
        },
        ["wal", "segment", "closed", start, end] => {
            let start = try!(usize::from_str(start).map_err(|_| error()));
            let end = try!(usize::from_str(end).map_err(|_| error()));
            let segment = try!(Segment::open(entry.path()));
            Ok(WalSegment::ClosedSegment(segment, start, end))
        },
        ["wal", "segment", "fresh"] => Ok(WalSegment::NewSegment(try!(Segment::open(entry.path())))),
        _ => Err(error()),
    }
}
