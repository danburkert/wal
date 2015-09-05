use std::fs::OpenOptions;
use std::io::{
    Error,
    ErrorKind,
    Result,
    Write,
};
use std::path::Path;
use std::sync::Arc;
use std::cell::UnsafeCell;
use std::slice;
use std::ops;

use byteorder::{ByteOrder, LittleEndian};
use crc::crc32;
use memmap::{Mmap, Protection};
use rand;

const SEGMENT_HEADER: &'static [u8; 4] = b"wal\0";
const SEGMENT_HEADER_LEN: usize = 8;
const ENTRY_HEADER_LEN: usize = 12;
const CRC32_LEN: usize = 4;

pub struct Entry {
    map: Arc<UnsafeCell<Mmap>>,
    /// The offset of the entry's data in the segment file.
    offset: usize,
    /// The length of the entry's data in the segment file.
    len: usize,
}

impl ops::Deref for Entry {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        unsafe {
            let ptr = (&*self.map.get()).ptr();
            slice::from_raw_parts(ptr.offset(self.offset as isize), self.len)
        }
    }
}

/// A single-file, append-only, durable log.
///
/// A segment has a fixed length set at creation. Once full, a segment will
/// reject append attempts. Entry length is only bounded by the amount of
/// available space, and the limits of `usize`. The number of entries is bounded
/// by the limits of `usize`.
pub struct Segment {
    /// The memory-mapped segment file.
    map: Arc<UnsafeCell<Mmap>>,

    /// An index holding the offset and length of each entry in the segment.
    index: Vec<(usize, usize)>,

    /// The last crc value written.
    crc: u32,
}

impl Segment {

    /// Opens the log segment at the specified path.
    pub fn open<P>(path: P) -> Result<Segment> where P: AsRef<Path> {
        let map = try!(Mmap::open_path(&path, Protection::ReadWrite));
        let mut segment = Segment { map: Arc::new(UnsafeCell::new(map)), index: Vec::new(), crc: 0};
        try!(segment.reindex());
        Ok(segment)
    }

    /// Creates a new log segment at the specified path with the provided
    /// initial capacity.
    ///
    /// The initial capacity must be at least four bytes.
    ///
    /// If a segment or another file already exists on the path it will be
    /// overwritten, and the allocated file space will be reused.
    ///
    /// # Segment Header Format
    ///
    /// | component              | type    |
    /// | ---------------------- | ------- |
    /// | magic bytes ("wal")    | 3 bytes |
    /// | segment format version | u8      |
    /// | random CRC seed        | u32     |
    ///
    /// The segment header is 8 bytes long: three magic bytes ("wal") followed
    /// by a segment format version u8, followed by a random u32 CRC seed. The
    /// CRC seed is serialized in little-endian format. The CRC seed ensures
    /// that if a segment file is overwritten with a new segment, the old
    /// segments entries will be ignored (since the CRC will not match).
    pub fn create<P>(path: P, capacity: usize) -> Result<Segment> where P: AsRef<Path> {
        if capacity < SEGMENT_HEADER_LEN {
            return Err(Error::new(ErrorKind::InvalidInput, "invalid segment capacity"));
        }

        let file = try!(OpenOptions::new()
                                    .read(true)
                                    .write(true)
                                    .create(true)
                                    .open(path));

        // Set the file's length and sync the metadata changes to disk.
        // TODO: this should be using posix_fallocate to avoid creating a sparse file.
        try!(file.set_len(capacity as u64));

        let mut map = try!(Mmap::open_with_offset(&file, Protection::ReadWrite, 0, capacity));

        let seed = rand::random();

        // Write and sync the header information.
        unsafe {
            try!((&mut map.as_mut_slice()[0..4]).write_all(SEGMENT_HEADER));
            LittleEndian::write_u32(&mut map.as_mut_slice()[4..], seed);
        }

        try!(file.sync_all());
        Ok(Segment { map: Arc::new(UnsafeCell::new(map)), index: Vec::new(), crc: seed })
    }

    /// Appends an entry to the segment. Returns the entry index, or `None` if
    /// the entry does not fit in the segment.
    ///
    /// # Entry Format
    ///
    /// | component           | type |
    /// | ------------------- | ---- |
    /// | length              | u64  |
    /// | CRC(length)         | u32  |
    /// | data                |      |
    /// | padding             |      |
    /// | CRC(data + padding) | u32  |
    ///
    /// Entries are serialized to the log with a fixed-length header, followed
    /// by the data itself, and finally a variable length footer. The header
    /// includes the length of the data followed by a CRC code of the length.
    /// The footer includes between 0 and 3 bytes of padding to align the data
    /// to a 4-byte boundary, followed by the CRC code of the data and padding.
    ///
    /// `u64` and `u32` values are serialized in little endian form.
    /// [CRC32-C](https://en.wikipedia.org/wiki/Cyclic_redundancy_check) is
    /// used.
    pub fn append<T>(&mut self, entry: &T) -> Result<Option<usize>>
    where T: ops::Deref<Target=[u8]> {
        if entry.len() > self.remaining() {
            return Ok(None);
        }

        let padding = padding(entry.len());
        let padded_len = entry.len() + padding;
        let total_len = ENTRY_HEADER_LEN + padded_len + CRC32_LEN;

        let offset = self.len();

        let buf = unsafe {
            slice::from_raw_parts_mut(self.map_mut().mut_ptr().offset(offset as isize), total_len)
        };

        LittleEndian::write_u64(buf, entry.len() as u64);
        let mut crc = crc32::update(self.crc, &crc32::CASTAGNOLI_TABLE, &buf[..8]);
        LittleEndian::write_u32(&mut buf[8..], crc);

        try!((&mut buf[ENTRY_HEADER_LEN..]).write_all(&*entry));

        if padding > 0 {
            let zeros: [u8; 4] = [0; 4];
            try!((&mut buf[ENTRY_HEADER_LEN + entry.len()..]).write_all(&zeros[..padding]));
        }
        crc = crc32::update(crc,
                            &crc32::CASTAGNOLI_TABLE,
                            &buf[ENTRY_HEADER_LEN..ENTRY_HEADER_LEN + padded_len]);
        LittleEndian::write_u32(&mut buf[ENTRY_HEADER_LEN + padded_len..], crc);
        self.index.push((offset + ENTRY_HEADER_LEN, entry.len()));
        self.crc = crc;

        Ok(Some(self.index.len() - 1))
    }

    /// Returns the entry Reads the entry at the index from the segment, or `None` if no such
    /// segment exists.
    ///
    pub fn read(&mut self, index: usize) -> Option<Entry> {
        self.index
            .get(index)
            .map(|&(offset, len)| {
                Entry { map: self.map.clone(),
                        offset: offset,
                        len: len }
            })
    }

    pub fn sync(&mut self) ->Result<()> {
        self.map_mut().flush()
    }

    /// Returns the capacity of the segment in bytes.
    ///
    /// Each entry is stored with a header and padding, so the entire capacity
    /// will not be available for entry data.
    pub fn capacity(&self) -> usize {
        self.map().len()
    }

    /// Number of bytes written to the file.
    pub fn len(&self) -> usize {
        self.index
            .last()
            .map(|&(offset, len)| offset + len + padding(len) + CRC32_LEN )
            .unwrap_or(SEGMENT_HEADER_LEN)
    }

    /// Returns the number of entries in the segment.
    pub fn count(&self) -> usize {
        self.index.len()
    }

    /// Returns the number of bytes available for the next segment.
    ///
    /// This method can be used to make sure that an entry can be appended to
    /// the segment without allocating additional file space.
    pub fn remaining(&self) -> usize {
        let unpadded = self.capacity() - self.len() - SEGMENT_HEADER_LEN - CRC32_LEN;
        // Round down to a multiple of four, since padding would otherwise be required.
        unpadded & !3
    }

    /// Reindex the segment.
    ///
    /// This need not be called in normal circumstances.
    pub fn reindex(&mut self) -> Result<()> {
        self.index.clear();

        let mut buf = unsafe { (&*self.map.get()).as_slice() };

        if &buf[..4] != SEGMENT_HEADER {
            return Err(Error::new(ErrorKind::InvalidData, "invalid segment header"));
        }
        self.crc = LittleEndian::read_u32(&buf[4..8]);

        let mut offset = SEGMENT_HEADER_LEN;

        buf = &buf[SEGMENT_HEADER_LEN..];

        while buf.len() >= ENTRY_HEADER_LEN + CRC32_LEN {
            let len = LittleEndian::read_u64(buf) as usize;
            let padding = padding(len);
            let padded_len = len + padding;

            let mut crc = crc32::update(self.crc, &crc32::CASTAGNOLI_TABLE, &buf[..8]);
            if crc != LittleEndian::read_u32(&buf[8..]) { break; }
            if ENTRY_HEADER_LEN + padded_len + CRC32_LEN > self.capacity() { break; }

            crc = crc32::update(crc,
                                &crc32::CASTAGNOLI_TABLE,
                                &buf[ENTRY_HEADER_LEN..ENTRY_HEADER_LEN + padded_len]);
            if crc != LittleEndian::read_u32(&buf[ENTRY_HEADER_LEN + padded_len..]) { break; }

            self.index.push((offset + ENTRY_HEADER_LEN, len));
            self.crc = crc;
            buf = &buf[ENTRY_HEADER_LEN + padded_len + CRC32_LEN..];
            offset += ENTRY_HEADER_LEN + padded_len + CRC32_LEN;
        }
        Ok(())
    }

    fn map(&self) -> &Mmap {
        unsafe { &*self.map.get() }
    }

    fn map_mut(&mut self) -> &mut Mmap {
        unsafe { &mut *self.map.get() }
    }

}

/// Returns the number of padding bytes to add to a buffer to ensure 4-byte alignment.
fn padding(len: usize) -> usize {
    0usize.wrapping_sub(len) & 3
}

#[cfg(test)]
mod test {
    extern crate tempdir;

    use std::io::Read;

    use super::{SEGMENT_HEADER_LEN, Segment, padding};

    #[test]
    fn test_pad_len() {
        assert_eq!(0, padding(0));

        assert_eq!(3, padding(1));
        assert_eq!(2, padding(2));
        assert_eq!(1, padding(3));
        assert_eq!(0, padding(4));

        assert_eq!(3, padding(5));
        assert_eq!(2, padding(6));
        assert_eq!(1, padding(7));
        assert_eq!(0, padding(8));

        assert_eq!(3, padding(9));
        assert_eq!(2, padding(10));
        assert_eq!(1, padding(11));
        assert_eq!(0, padding(12));

        assert_eq!(3, padding(13));
        assert_eq!(2, padding(14));
        assert_eq!(1, padding(15));
        assert_eq!(0, padding(16));
    }

    fn test_segment(len: usize) -> (Segment, tempdir::TempDir) {
        let dir = tempdir::TempDir::new("segment").unwrap();
        let mut path = dir.path().to_path_buf();
        path.push("test-segment");
        (Segment::create(path, len).unwrap(), dir)
    }

    #[test]
    fn test_create() {
        let segment = test_segment(4096).0;
        assert_eq!(SEGMENT_HEADER_LEN, segment.len());
        assert_eq!(4096, segment.capacity());
        assert_eq!(0, segment.count());
    }

    #[test]
    fn test_entries() {
        let mut segment = test_segment(4096).0;
        let entries: &[&[u8]] = &[b"",
                                  b"0",
                                  b"01",
                                  b"012",
                                  b"0123",
                                  b"01234",
                                  b"012345",
                                  b"0123456",
                                  b"01234567",
                                  b"012345678",
                                  b"0123456789"];

        for (index, entry) in entries.iter().enumerate() {
            assert_eq!(index, segment.append(entry).unwrap().unwrap());
        }

        for index in 0..entries.len() {
            assert_eq!(entries[index], &*segment.read(index).unwrap());
        }
    }

    #[test]
    fn test_open() {
        let dir = tempdir::TempDir::new("segment").unwrap();
        let mut path = dir.path().to_path_buf();
        path.push("test-open");

        let entries: &[&[u8]] = &[b"",
                                  b"0",
                                  b"01",
                                  b"012",
                                  b"0123",
                                  b"01234",
                                  b"012345",
                                  b"0123456",
                                  b"01234567",
                                  b"012345678",
                                  b"0123456789"];

        {
            let mut segment = Segment::create(&path, 4096).unwrap();
            for (index, entry) in entries.iter().enumerate() {
                assert_eq!(index, segment.append(entry).unwrap().unwrap());
            }
        }

        let mut segment = Segment::open(&path).unwrap();
        assert_eq!(4096, segment.capacity());
        assert_eq!(entries.len(), segment.count());

        for index in 0..entries.len() {
            assert_eq!(entries[index], &*segment.read(index).unwrap());
        }
    }

    /// Tests that when overwriting an existing segment file with a new segment,
    /// the old entries will not be indexed.
    #[test]
    fn test_overwrite() {
        let dir = tempdir::TempDir::new("segment").unwrap();
        let mut path = dir.path().to_path_buf();
        path.push("test-overwrite");

        let entries: &[&[u8]] = &[b"abcdefgh",
                                  b"abcdefgh",
                                  b"abcdefgh"];

        {
            let mut segment = Segment::create(&path, 4096).unwrap();
            for (index, entry) in entries.iter().enumerate() {
                assert_eq!(index, segment.append(entry).unwrap().unwrap());
            }
        }

        let mut segment = Segment::create(&path, 4096).unwrap();
        segment.reindex().unwrap();
        assert_eq!(0, segment.count());
    }
}
