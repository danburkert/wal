use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::{
    Error,
    ErrorKind,
    Result,
    Write,
};
use std::mem;
use std::ops::Deref;
use std::ops;
use std::path::{Path, PathBuf};
use std::ptr;
use std::thread;

use byteorder::{ByteOrder, LittleEndian};
use crc::crc32;
use eventual::{Async, Future};
use memmap::{Mmap, Protection, MmapViewSync};
use rand;

/// The magic bytes and version tag of the segment header.
const SEGMENT_HEADER: &'static [u8; 4] = b"wal\0";

/// The length of both the segment and entry header.
const HEADER_LEN: usize = 8;

/// The length of a CRC value.
const CRC_LEN: usize = 4;

type Crc = u32;

pub struct Entry {
    view: MmapViewSync,
}

impl Deref for Entry {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        unsafe { self.view.as_slice() }
    }
}

impl fmt::Debug for Entry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Entry {{ len: {} }}", self.view.len())
    }
}

/// An append-only, fixed-length, durable, on-disk container of entries.
///
/// The segment on-disk format is as simple as possible, while providing
/// backwards compatibility, protection against corruption, and alignment
/// guarantees.
///
/// A version tag allows the internal format to be updated, while still
/// maintaining compatibility with previously written files. CRCs are used
/// to ensure that entries are not corrupted. Padding is used to ensure that
/// entries are always aligned to 8-byte boundaries.
///
/// ## On Disk Format
///
/// All version, length, and CRC integers are serialized in little-endian format.
///
/// All CRC values are computed using
/// [CRC32-C](https://en.wikipedia.org/wiki/Cyclic_redundancy_check).
///
/// ### Segment Header Format
///
/// | component              | type    |
/// | ---------------------- | ------- |
/// | magic bytes ("wal")    | 3 bytes |
/// | segment format version | u8      |
/// | random CRC seed        | u32     |
///
/// The segment header is 8 bytes long: three magic bytes ("wal") followed by a
/// segment format version `u8`, followed by a random `u32` CRC seed. The CRC
/// seed is serialized in little-endian format. The CRC seed ensures that if a
/// segment file is reused for a new segment, the old entries will be ignored
/// (since the CRC will not match).
///
/// ### Entry Format
///
/// | component                    | type |
/// | ---------------------------- | ---- |
/// | length                       | u64  |
/// | data                         |      |
/// | padding                      |      |
/// | CRC(length + data + padding) | u32  |
///
/// Entries are serialized to the log with a fixed-length header, followed by
/// the data itself, and finally a variable length footer. The header includes
/// the length of the data as a u64 in little-endian format. The footer includes
/// between 0 and 7 bytes of padding to extend the total length of the entry to
/// a multiple of 8, followed by the CRC code of the length, data, and padding.
pub struct Segment {
    /// The segment file buffer.
    mmap: MmapViewSync,
    /// The segment file path.
    path: PathBuf,
    /// Index of entry offset and lengths.
    index: Vec<(usize, usize)>,
    /// The crc of the last appended entry.
    crc: Crc,
    /// Offset of last flush.
    flush_offset: usize,
}

impl Segment {

    /// Creates a new segment file at the specified path with the provided
    /// initial capacity.
    ///
    /// The initial capacity must be at least 8 bytes.
    ///
    /// If a file already exists at the path it will be overwritten, and the
    /// allocated space will be reused.
    ///
    /// An individual file should only be opened by a single segment at a time.
    ///
    /// The caller is responsible for flushing the directory in order to
    /// guarantee that the segment is durable in the event of a crash.
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

        let mut mmap = try!(Mmap::open_with_offset(&file,
                                                   Protection::ReadWrite,
                                                   0,
                                                   capacity)).into_view_sync();
        let seed = rand::random();

        // Write and flush the header information.
        copy_memory(SEGMENT_HEADER, unsafe { &mut mmap.as_mut_slice()[..4] });
        LittleEndian::write_u32(unsafe { &mut mmap.as_mut_slice()[4..] }, seed);

        Ok(Segment {
            mmap: mmap,
            path: path.as_ref().to_path_buf(),
            index: Vec::new(),
            crc: seed,
            flush_offset: 0,
        })
    }

    /// Opens the segment file at the specified path.
    ///
    /// An individual file should only be opened by one segment at a time.
    pub fn open<P>(path: P) -> Result<Segment> where P: AsRef<Path> {
        let mmap = try!(Mmap::open_path(&path, Protection::ReadWrite)).into_view_sync();
        let mut index = Vec::new();
        let mut crc;
        {
            // Parse the segment, filling out the index containing the offset
            // and length of each entry, as well as the latest CRC value.
            //
            // If the CRC of any entry does not match, then parsing stops and
            // the remainder of the file is considered empty.
            let segment = unsafe { mmap.as_slice() };
            crc = LittleEndian::read_u32(&segment[SEGMENT_HEADER.len()..]);
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
        }

        Ok(Segment {
            mmap: mmap,
            path: path.as_ref().to_path_buf(),
            index: index,
            crc: crc,
            flush_offset: 0,
        })
    }

    fn as_slice(&self) -> &[u8] {
        unsafe { self.mmap.as_slice() }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { self.mmap.as_mut_slice() }
    }

    fn flush_offset(&self) -> usize {
        self.flush_offset
    }

    fn set_flush_offset(&mut self, offset: usize) {
        self.flush_offset = offset;
    }

    /// Returns the segment entry at the specified index, or `None` if no such
    /// entry exists.
    pub fn entry(&self, index: usize) -> Option<Entry> {
        self.index
            .get(index)
            .map(|&(offset, len)| {
                let mut view = unsafe { self.mmap.clone() };
                // restrict only fails on bounds errors, but an invariant of
                // Segment is that the index always holds valid offset and
                // length bounds.
                view.restrict(offset, len).unwrap();
                Entry { view: view }
            })
    }

    /// Appends an entry to the segment, returning the index of the new entry,
    /// or `None` if there is insufficient space for the entry.
    ///
    /// The entry may be immediately read from the log, but it is not guaranteed
    /// to be durably stored on disk until the segment is successfully flushed.
    pub fn append<T>(&mut self, entry: &T) -> Option<usize> where T: ops::Deref<Target=[u8]> {
        let remaining_size = self.remaining_size();
        if remaining_size == 0 || entry.len() > self.remaining_size() {
            debug!("{:?}: insufficient remaining capacity to append {} byte entry",
                   self, entry.len());
            return None;
        }
        info!("{:?}: appending {} byte entry", self, entry.len());

        let padding = padding(entry.len());
        let padded_len = entry.len() + padding;

        let offset = self.size();

        let mut crc = self.crc;

        LittleEndian::write_u64(&mut self.as_mut_slice()[offset..], entry.len() as u64);
        copy_memory(entry.deref(), &mut self.as_mut_slice()[offset + HEADER_LEN..]);

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

    /// Truncates the entries in the segment beginning with `from`.
    ///
    /// The entries are not guaranteed to be removed until the segment is
    /// successfully flushed.
    pub fn truncate(&mut self, from: usize) {
        if from >= self.index.len() { return; }

        // Remove the index entries.
        let _ = self.index.drain(from..);

        // And overwrite the existing data so that we will not read the data back after a crash.
        let size = self.size();
        let zeroes: [u8; 16] = [0; 16];
        copy_memory(&zeroes, &mut self.as_mut_slice()[size..]);
    }

    /// Flushes recently written entries to durable storage.
    pub fn flush(&mut self) -> Result<()> {
        let start = self.flush_offset;
        let end = self.size();
        assert!(start <= end);

        if start == end {
            Ok(())
        } else {
            let mut view = unsafe { self.mmap.clone() };
            try!(view.restrict(start, end - start));
            view.flush()
        }
    }

    /// Flushes recently written entries to durable storage.
    pub fn flush_async(&mut self) -> Future<(), Error> {
        let start = self.flush_offset();
        let end = self.size();
        assert!(start <= end);

        if start == end {
            Future::of(())
        } else {
            let mut view = unsafe { self.mmap.clone() };
            self.set_flush_offset(end);
            let (complete, future) = Future::pair();
            thread::spawn(move || {
                match view.restrict(start, end - start).and_then(|_| view.flush()) {
                    Ok(_) => complete.complete(()),
                    Err(error) => complete.fail(error),
                }
            });
            future
        }
    }

    pub fn ensure_capacity(&mut self, entry_size: usize) -> Result<()> {
        let capacity = entry_size + padding(entry_size) + HEADER_LEN + CRC_LEN + self.size();
        if capacity > self.capacity() {
            info!("{:?}: extending capacity to {} for entry of size {}",
                  self, capacity, entry_size);
            try!(self.flush());
            let file = try!(OpenOptions::new()
                                        .read(true)
                                        .write(true)
                                        .create(false)
                                        .open(&self.path));
            try!(file.set_len(capacity as u64));

            let mut mmap = try!(Mmap::open_with_offset(&file,
                                                       Protection::ReadWrite,
                                                       0,
                                                       capacity)).into_view_sync();
            mem::swap(&mut mmap, &mut self.mmap);
        }
        Ok(())
    }

    /// Returns the number of entries in the segment.
    pub fn len(&self) -> usize {
        self.index.len()
    }

    /// Returns the capacity of the segment in bytes.
    ///
    /// Each entry is stored with a header and padding, so the entire capacity
    /// will not be available for entry data.
    pub fn capacity(&self) -> usize {
        self.mmap.len()
    }

    /// Returns the total number of bytes used to store existing entries,
    /// including header and padding overhead.
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
        (self.capacity() & !7).saturating_sub(self.size())
                              .saturating_sub(HEADER_LEN)
                              .saturating_sub(CRC_LEN)
    }

    /// Returns the path to the segment file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Renames the segment file.
    ///
    /// The caller is responsible for syncing the directory in order to
    /// guarantee that the rename is durable in the event of a crash.
    pub fn rename<P>(&mut self, path: P) -> Result<()> where P: AsRef<Path> {
        try!(fs::rename(&self.path, &path));
        self.path = path.as_ref().to_path_buf();
        Ok(())
    }

    /// Deletes the segment file.
    pub fn delete(self) -> Result<()> {
        fs::remove_file(&self.path)
    }
}

impl fmt::Debug for Segment {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Segment {{ path: {:?}, entries: {}, space: ({}/{}) }}",
               &self.path, self.index.len(), self.size(), self.capacity())
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

/// Returns the overhead of storing an entry of length `len`.
pub fn entry_overhead(len: usize) -> usize {
    padding(len) + HEADER_LEN + CRC_LEN
}

/// Returns the fixed-overhead of segment metadata.
pub fn segment_overhead() -> usize {
    HEADER_LEN
}

#[cfg(test)]
mod test {

    use std::io::ErrorKind;

    use env_logger;
    use tempdir;

    use super::{Segment, padding};

    use test_utils::EntryGenerator;

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

    fn create_segment(len: usize) -> Segment {
        let dir = tempdir::TempDir::new("segment").unwrap();
        let mut path = dir.path().to_path_buf();
        path.push("sync-segment");
        Segment::create(path, len).unwrap()
    }

    /// Checks that entries can be appended to a segment.
    fn check_append(segment: &mut Segment) {
        let _ = env_logger::init();
        assert_eq!(0, segment.len());

        let entries: Vec<Vec<u8>> = EntryGenerator::with_segment_capacity(segment.capacity())
                                                   .into_iter()
                                                   .collect();

        for (idx, entry) in entries.iter().enumerate() {
            assert_eq!(idx, segment.append(entry).unwrap());
            assert_eq!(&entry[..], &*segment.entry(idx).unwrap());
        }

        for (idx, entry) in entries.iter().enumerate() {
            assert_eq!(&entry[..], &*segment.entry(idx).unwrap());
        }
    }

    #[test]
    fn test_append() {
        check_append(&mut create_segment(8));
        check_append(&mut create_segment(9));
        check_append(&mut create_segment(32));
        check_append(&mut create_segment(100));
        check_append(&mut create_segment(1023));
        check_append(&mut create_segment(1024));
        check_append(&mut create_segment(1025));
        check_append(&mut create_segment(4096));
        check_append(&mut create_segment(8 * 1024 * 1024));
    }

    #[test]
    fn test_create_dir_path() {
        let _ = env_logger::init();
        let dir = tempdir::TempDir::new("segment").unwrap();
        assert!(Segment::open(dir.path()).is_err());
    }

    #[test]
    fn test_entries() {
        let _ = env_logger::init();
        let mut segment = create_segment(4096);
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

    /// Tests that opening a non-existent segment file will fail.
    #[test]
    fn test_open_nonexistent() {
        let _ = env_logger::init();
        let dir = tempdir::TempDir::new("segment").unwrap();
        let mut path = dir.path().to_path_buf();
        path.push("test-open-nonexistent");
        assert_eq!(ErrorKind::NotFound, Segment::open(&path).unwrap_err().kind());
    }
}
