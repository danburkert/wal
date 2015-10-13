//! An append-only, fixed-length, durable log of arbitrary entries.
//!
//! The segment on-disk format is as simple as possible, while providing
//! backwards compatibility, protection against corruption, and alignment
//! guarantees.
//!
//! A version tag allows the internal format to be updated, while still
//! maintaining compatibility with previously written files. CRCs are used
//! to ensure that entries are not corrupted. Padding is used to ensure that
//! entries are always aligned to 8-byte boundaries.
//!
//! ## On Disk Format
//!
//! All version, length, and CRC integers are serialized in little-endian format.
//!
//! All CRC values are computed using
//! [CRC32-C](https://en.wikipedia.org/wiki/Cyclic_redundancy_check).
//!
//! ### Segment Header Format
//!
//! | component              | type    |
//! | ---------------------- | ------- |
//! | magic bytes ("wal")    | 3 bytes |
//! | segment format version | u8      |
//! | random CRC seed        | u32     |
//!
//! The segment header is 8 bytes long: three magic bytes ("wal") followed
//! by a segment format version u8, followed by a random u32 CRC seed. The
//! CRC seed is serialized in little-endian format. The CRC seed ensures
//! that if a segment file is overwritten with a new segment, the old
//! segments entries will be ignored (since the CRC will not match).
//!
//! # Entry Format
//!
//! | component                    | type |
//! | ---------------------------- | ---- |
//! | length                       | u64  |
//! | data                         |      |
//! | padding                      |      |
//! | CRC(length + data + padding) | u32  |
//!
//! Entries are serialized to the log with a fixed-length header, followed
//! by the data itself, and finally a variable length footer. The header
//! includes the length of the data followed by a CRC code of the length.
//! The footer includes between 0 and 7 bytes of padding to extend the length
//! of the entry to a multiple of 8, followed by the CRC code of the length,
//! data, and padding.

use std::fs::{self, File, OpenOptions};
use std::io::{
    Error,
    ErrorKind,
    Result,
    Write,
};
use std::ops;
use std::path::{Path, PathBuf};
use std::ptr;

use byteorder::{ByteOrder, LittleEndian};
use crc::crc32;
use memmap::{Mmap, Protection};
use rand;
use mmap::MmapHandle;

/// The magic bytes and version tag of the segment header.
const SEGMENT_HEADER: &'static [u8; 4] = b"wal\0";

/// The length of both the segment and entry header.
const HEADER_LEN: usize = 8;

/// The length of a CRC value.
const CRC_LEN: usize = 4;

type Crc = u32;

/// A read-only segment.
pub struct Segment {
    /// The segment file buffer.
    mmap: MmapHandle,
    /// The open segment file.
    file: File,
    /// The segment file path.
    path: PathBuf,
    /// Index of entry offset and lengths.
    index: Vec<(usize, usize)>,
    /// The crc of the last appended entry.
    crc: u32,
}

impl Segment {

    /// Creates a new log segment at the specified path with the provided
    /// initial capacity.
    ///
    /// The initial capacity must be at least 8 bytes.
    ///
    /// If a segment or another file already exists on the path it will be
    /// overwritten, and the allocated file space will be reused.
    pub fn create<P>(path: P, capacity: usize) -> Result<Segment> where P: AsRef<Path> {
        if capacity < HEADER_LEN {
            return Err(Error::new(ErrorKind::InvalidInput, "invalid segment capacity"));
        }

        let file = try!(OpenOptions::new()
                                    .read(true)
                                    .write(true)
                                    .create(true)
                                    .open(&path));

        try!(file.set_len(capacity as u64));
        // TODO: sync directory

        let mut mmap = MmapHandle::new(try!(Mmap::open_with_offset(&file,
                                                                   Protection::ReadWrite,
                                                                   0,
                                                                   capacity)));

        let seed = rand::random();

        // Write and sync the header information.
        copy_memory(SEGMENT_HEADER, unsafe { &mut mmap.as_mut().as_mut_slice()[..4] });
        LittleEndian::write_u32(unsafe { &mut mmap.as_mut().as_mut_slice()[4..] }, seed);

        try!(file.sync_all());
        Ok(Segment {
            mmap: mmap,
            file: file,
            path: path.as_ref().to_path_buf(),
            index: Vec::new(),
            crc: seed,
        })
    }

    /// Opens the segment at the specified path.
    pub fn open<P>(path: P) -> Result<Segment> where P: AsRef<Path> {
        let mmap = MmapHandle::new(try!(Mmap::open_path(&path, Protection::ReadWrite)));
        let file = try!(OpenOptions::new()
                                    .read(true)
                                    .write(true)
                                    .create(false)
                                    .open(&path));
        let (index, crc) = try!(rebuild_index(unsafe { mmap.as_ref().as_slice() }));
        Ok(Segment {
            mmap: mmap,
            file: file,
            path: path.as_ref().to_path_buf(),
            index: index,
            crc: crc,
        })
    }

    /// Appends an entry to the segment, returning `None` if there is
    /// insufficient space in the log for the entry, or the index of the new
    /// entry.
    ///
    /// The entry may be immediately read from the log, but it is not guaranteed
    /// to be durably stored on disk until the segment is successfully flushed.
    pub fn append<T>(&mut self, entry: &T) -> Option<usize>
    where T: ops::Deref<Target=[u8]> {
        if entry.len() > self.remaining_size() {
            return None;
        }

        let padding = padding(entry.len());
        let padded_len = entry.len() + padding;

        let offset = self.size();

        let mut crc = self.crc;

        LittleEndian::write_u64(&mut self.as_mut_slice()[offset..],
                                entry.len() as u64);
        copy_memory(entry, &mut self.as_mut_slice()[offset + HEADER_LEN..]);

        if padding > 0 {
            let zeros: [u8; 8] = [0; 8];
            copy_memory(&zeros[..padding],
                        &mut self.as_mut_slice()[offset + HEADER_LEN + entry.len()..]);
        }

        crc = crc32::update(crc, &crc32::CASTAGNOLI_TABLE,
                            &self.as_slice()[offset..offset + HEADER_LEN + padded_len]);

        LittleEndian::write_u32(&mut self.as_mut_slice()[offset + HEADER_LEN + padded_len..], crc);

        self.crc = crc;
        self.index.push((offset + HEADER_LEN, entry.len()));
        Some(self.index.len() - 1)
    }

    /// Returns the segment entry at the specified index, or `None` if no such
    /// entry exists.
    pub fn entry(&self, index: usize) -> Option<&[u8]> {
        self.index
            .get(index)
            .map(|&(offset, len)| &self.as_slice()[offset..offset + len])
    }

    /// Returns the capacity of the segment in bytes.
    ///
    /// Each entry is stored with a header and padding, so the entire capacity
    /// will not be available for entry data.
    pub fn capacity(&self) -> usize {
        self.mmap.as_ref().len()
    }

    /// Returns the total number of bytes used to store the entries, including
    /// header and padding overhead.
    pub fn size(&self) -> usize {
        self.index
            .last()
            .map(|&(offset, len)| offset + len + padding(len) + CRC_LEN )
            .unwrap_or(HEADER_LEN)
    }

    /// Returns the number of bytes available for the next segment.
    ///
    /// This method can be used to make sure that an entry can be appended to
    /// the segment without allocating additional file space.
    pub fn remaining_size(&self) -> usize {
        let unpadded = self.capacity() - self.size() - HEADER_LEN - CRC_LEN;
        // Round down to a multiple of eight, since padding would otherwise be required.
        unpadded & !7
    }

    pub fn delete(self) -> Result<()> {
        let Segment { path, .. } = self;
        fs::remove_file(path)
    }

    /// Renames the segment.
    ///
    /// The caller is responsible for syncing the directory in order to
    /// guarantee that the rename is durable.
    pub fn rename<P>(&mut self, path: P) -> Result<()> where P: AsRef<Path> {
        try!(fs::rename(&self.path, &path));
        self.path = path.as_ref().to_path_buf();
        Ok(())
    }

    /// Returns the number of entries in the segment.
    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn flush(&mut self) -> Result<()> {
        self.mmap.as_mut().flush()
    }

    fn as_slice(&self) -> &[u8] {
        unsafe { self.mmap.as_ref().as_slice() }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { self.mmap.as_mut().as_mut_slice() }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

/// Copies data from `src` to `dst`
///
/// Panics if the length of `dst` is less than the length of `src`.
pub fn copy_memory(src: &[u8], dst: &mut [u8]) {
    let len_src = src.len();
    assert!(dst.len() >= len_src);
    unsafe {
        ptr::copy_nonoverlapping(src.as_ptr(),
                                 dst.as_mut_ptr(),
                                 len_src);
    }
}

/// Returns the number of padding bytes to add to a buffer to ensure 8-byte alignment.
fn padding(len: usize) -> usize {
    4usize.wrapping_sub(len) & 7
}

/// Parses a segment, returning an index containing the offset and length of
/// each entry, as well as the latest CRC value.
///
/// If the CRC of any entry does not match, then parsing stops and the entries
/// up until that point are returned.
fn rebuild_index(segment: &[u8]) -> Result<(Vec<(usize, usize)>, Crc)> {
    if segment.len() < HEADER_LEN {
        return Err(Error::new(ErrorKind::InvalidData, "invalid segment length"));
    }

    if &segment[..SEGMENT_HEADER.len()] != SEGMENT_HEADER {
        return Err(Error::new(ErrorKind::InvalidData, "invalid segment header"));
    }

    let mut index = Vec::new();
    let mut crc = LittleEndian::read_u32(&segment[SEGMENT_HEADER.len()..]);
    let mut offset = HEADER_LEN;

    while segment.len() >= offset + HEADER_LEN + CRC_LEN {
        let len = LittleEndian::read_u64(&segment[offset..]) as usize;
        let padding = padding(len);
        let padded_len = len + padding;
        if offset + HEADER_LEN + padded_len + CRC_LEN > segment.len() { break; }

        let entry_crc = crc32::update(crc,
                                      &crc32::CASTAGNOLI_TABLE,
                                      &segment[offset..offset + HEADER_LEN + padded_len]);
        if entry_crc != LittleEndian::read_u32(&segment[offset + HEADER_LEN + padded_len..]) { break; }

        crc = entry_crc;
        index.push((offset + HEADER_LEN, len));
        offset += HEADER_LEN + padded_len + CRC_LEN;
    }
    Ok((index, crc))
}

#[cfg(test)]
mod test {
    extern crate tempdir;
    extern crate env_logger;

    use super::{HEADER_LEN, Segment, padding};

    #[test]
    fn test_pad_len() {
        assert_eq!(4, padding(0));
        assert_eq!(3, padding(1));
        assert_eq!(2, padding(2));
        assert_eq!(1, padding(3));
        assert_eq!(0, padding(4));
        assert_eq!(7, padding(5));
        assert_eq!(6, padding(6));
        assert_eq!(5, padding(7));

        assert_eq!(4, padding(8));
        assert_eq!(3, padding(9));
        assert_eq!(2, padding(10));
        assert_eq!(1, padding(11));
        assert_eq!(0, padding(12));
        assert_eq!(7, padding(13));
        assert_eq!(6, padding(14));
        assert_eq!(5, padding(15));
    }

    fn test_segment(len: usize) -> (Segment, tempdir::TempDir) {
        let _ = env_logger::init();
        let dir = tempdir::TempDir::new("segment").unwrap();
        let mut path = dir.path().to_path_buf();
        path.push("test-segment");
        (Segment::create(path, len).unwrap(), dir)
    }

    #[test]
    fn test_create() {
        let _ = env_logger::init();
        let segment = test_segment(4096).0;
        assert_eq!(HEADER_LEN, segment.size());
        assert_eq!(4096, segment.capacity());
        assert_eq!(0, segment.len());
    }

    #[test]
    fn test_entries() {
        let _ = env_logger::init();
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
            assert_eq!(index, segment.append(entry).unwrap());
        }

        for index in 0..entries.len() {
            assert_eq!(entries[index], &*segment.entry(index).unwrap());
        }
    }

    #[test]
    fn test_open() {
        let _ = env_logger::init();
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
            for (i, entry) in entries.iter().enumerate() {
                let index = segment.append(entry).unwrap();
                assert_eq!(i, index);
            }
            segment.flush().unwrap();
        }

        let segment = Segment::open(&path).unwrap();
        assert_eq!(4096, segment.capacity());
        assert_eq!(entries.len(), segment.len());

        for index in 0..entries.len() {
            assert_eq!(entries[index], &*segment.entry(index).unwrap());
        }
    }

    /// Tests that when overwriting an existing segment file with a new segment,
    /// the old entries will not be indexed.
    #[test]
    fn test_overwrite() {
        let _ = env_logger::init();
        let dir = tempdir::TempDir::new("segment").unwrap();
        let mut path = dir.path().to_path_buf();
        path.push("test-overwrite");

        let entries: &[&[u8]] = &[b"abcdefgh",
                                  b"abcdefgh",
                                  b"abcdefgh"];

        {
            let mut segment = Segment::create(&path, 4096).unwrap();
            for (i, entry) in entries.iter().enumerate() {
                let index = segment.append(entry).unwrap();
                assert_eq!(i, index);
            }
        }

        Segment::create(&path, 4096).unwrap();
        let segment = Segment::open(&path).unwrap();

        assert_eq!(0, segment.len());
    }
}
