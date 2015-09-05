extern crate wal;

use wal::mmap_segment::Segment;
use std::mem;


fn main() {
    mem::forget(Segment::create("test", 4096).unwrap());
}
