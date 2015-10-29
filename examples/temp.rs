extern crate wal;

use wal::SyncSegment;
use std::mem;


fn main() {
    mem::forget(SyncSegment::create("test", 4096).unwrap());
}
