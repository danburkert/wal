use std::cell::UnsafeCell;
use std::sync::Arc;

use memmap::Mmap;

/// A cloneable handle to a memory map.
///
/// Allowing mutable access to a memory map is, in general, unsafe.
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
