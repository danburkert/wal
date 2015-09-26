#![feature(drain)]

extern crate byteorder;
extern crate crc;
extern crate eventual;
extern crate memmap;
extern crate rand;
#[macro_use]
extern crate log;

pub mod mmap;



/*
/// An write ahead log supporting efficient appends, truncation, and prefix truncation.
struct Wal {

    /// The segment currently being written to.
    active: (usize, Segment),

    /// The first index of the segment.
    active_from: usize,

    archive: Vec<(usize, Segment)>,

    inactive: BinaryHeap<Segment>,
}
*/

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
