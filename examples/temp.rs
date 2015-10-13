extern crate wal;

use wal::Segment;
use std::mem;


fn main() {
    mem::forget(Segment::create("test", 4096).unwrap());
}
