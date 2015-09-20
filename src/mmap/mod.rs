use std::sync::Arc;
use std::cell::UnsafeCell;

mod segment;
mod flusher;

use memmap::Mmap;

pub use self::segment::Segment;

pub struct Map {
    map: Arc<UnsafeCell<Mmap>>,
}

impl Map {
    pub fn new(map: Mmap) -> Map {
        Map {
            map: Arc::new(UnsafeCell::new(map)),
        }
    }

    pub fn get(&self) -> &Mmap {
        unsafe { &*self.map.get() }
    }

    pub fn get_mut(&mut self) -> &mut Mmap {
        unsafe { &mut *self.map.get() }
    }
}

unsafe impl Send for Map { }
