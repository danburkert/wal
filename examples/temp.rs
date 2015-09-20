extern crate wal;

use wal::mmap::Segment;
use std::mem;


fn main() {
    mem::forget(Segment::create("test", 4096).unwrap());
}
