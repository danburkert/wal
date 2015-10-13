mod flusher;
mod segment;
mod creator;

use std::cell::UnsafeCell;
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::fs;
use std::io::{Error, ErrorKind, Result};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use memmap::Mmap;

pub use self::creator::SegmentCreator;
pub use self::flusher::SegmentFlusher;
pub use self::segment::Segment;

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

/// An open segment and its ID.
struct OpenSegment {
    pub id: u64,
    pub segment: Segment,
}

/// A closed segment, and the associated start and stop indices.
struct ClosedSegment {
    pub start_index: u64,
    pub end_index: u64,
    pub segment: Segment,
}

enum WalSegment {
    Open(OpenSegment),
    Closed(ClosedSegment),
}

pub struct Wal {
    open_segment: OpenSegment,
    closed_segments: VecDeque<ClosedSegment>,
    flusher: SegmentFlusher,
    creator: SegmentCreator,
}

impl Wal {
    pub fn open<P>(path: P) -> Result<Wal> where P: AsRef<Path> {
        // Holds open segments in the directory.
        let mut open_segments: Vec<OpenSegment> = Vec::new();
        let mut closed_segments: Vec<ClosedSegment> = Vec::new();

        for entry in try!(fs::read_dir(path)) {
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



        /*
        struct Wal {
            open_segment: open_segment,
            closed_segments: closed_segments.into_iter().collect(),
            flusher: Flusher,
            creator: SegmentCreator,
        }
        */
        unimplemented!()
    }
}

fn close_segment(OpenSegment { segment, .. }: OpenSegment, start_index: u64) -> Result<ClosedSegment> {
    let end_index = start_index + segment.len() as u64;

    unimplemented!()

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
        ["segment", "open", id] => {
            let id = try!(u64::from_str(id).map_err(|_| error()));
            let segment = try!(Segment::open(entry.path()));
            Ok(WalSegment::Open(OpenSegment { segment: segment, id: id }))
        },
        ["segment", "closed", start, end] => {
            let start = try!(u64::from_str(start).map_err(|_| error()));
            let end = try!(u64::from_str(end).map_err(|_| error()));
            let segment = try!(Segment::open(entry.path()));
            Ok(WalSegment::Closed(ClosedSegment { start_index: start,
                                                  end_index: end,
                                                  segment: segment }))
        },
        _ => Err(error()),
    }
}
