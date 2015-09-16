#![feature(drain)]

extern crate byteorder;
extern crate crc;
extern crate eventual;
extern crate memmap;
extern crate rand;
#[macro_use]
extern crate log;

pub mod io;
pub mod mmap;

use std::collections::BinaryHeap;

use mmap::Segment;

/// A sendable memory map. Mutably sharing a memory map between threads allows
/// mutable aliasing of memory, so we are careful to split memory maps into
/// immutable (shareable), and mutable sections.
///
/// The `Segment` type uses `Map` to concurrently append to the file and flush
/// changes.
#[derive(Clone)]
struct Map {
    map: Arc<UnsafeCell<Mmap>>,
}

impl Map {
    /// Creates a new cloneable memory map from the memory map.
    pub fn new(map: Mmap) -> Map {
        Map {
            map: Arc::new(UnsafeCell::new(map)),
        }
    }

    /// Gets a shared reference to the memory map.
    pub fn get(&self) -> &Mmap {
        unsafe { &*self.map.get() }
    }

    /// Gets a mutable reference to the memory map.
    pub fn get_mut(&mut self) -> &mut Mmap {
        unsafe { &mut *self.map.get() }
    }
}

unsafe impl Send for Map { }

trait Flusher {
    fn flush(map: Map, offset: usize, len: usize);
}

/// An write ahead log supporting efficient appends, truncation, and prefix truncation.
struct Wal {

    /// The segment currently being written to.
    active: (usize, Segment),

    /// The first index of the segment.
    active_from: usize,

    archive: Vec<(usize, Segment)>,

    inactive: BinaryHeap<Segment>,
}

// active
// archive
// hopper

// write segment
// read segments
// inactive segments
// snapshot

//impl Wal {
    //pub fn append<T>(&mut self, entry: &T) -> Result<Option<usize>>
    //where T: ops::Deref<Target=[u8]> {
//}
