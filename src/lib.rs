extern crate byteorder;
extern crate crc;
extern crate eventual;
extern crate memmap;
extern crate rand;
#[macro_use]
extern crate log;

pub mod io;
pub mod mmap;



//struct Wal {
    //active: mmap::Segment,
    //flushed: usize,
    //written: usize,
//}

//impl Wal {

    //pub fn append<T>(&mut self, entry: &T) -> Result<Option<usize>>
    //where T: ops::Deref<Target=[u8]> {

//}
