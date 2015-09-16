use std::sync::Arc;
use std::cell::UnsafeCell;

mod segment;
mod flusher;

use memmap::Mmap;

pub use self::segment::Segment;
pub use self::flusher::Flusher;
