use std::cmp;
use std::fs::{File, OpenOptions};
use std::io::{
    BufRead,
    BufReader,
    Error,
    ErrorKind,
    Read,
    Result,
    Seek,
    SeekFrom,
    Take,
    Write,
};
use std::path::Path;
use std::usize;

use byteorder::{ByteOrder, LittleEndian, ReadBytesExt, WriteBytesExt};
use crc::crc32;
use rand;

const SEGMENT_HEADER: &'static [u8; 4] = b"wal\0";
const SEGMENT_HEADER_LEN: u64 = 8;
const ENTRY_HEADER_LEN: u64 = 12;
const CRC32_LEN: u64 = 4;

#[derive(Debug)]
struct Entry {
    /// The offset of the entry's data in the segment file.
    offset: u64,

    /// The length of the entry's data in the segment file.
    len: usize,
}

/// A single-file, append-only, durable log.
///
/// A segment has a fixed length set at creation. Once full, a segment will
/// reject append attempts. Entry length is only bounded by the amount of
/// available space, and the limits of `usize`. The number of entries is bounded
/// by the limits of `usize`.
pub struct Segment {
    /// The file containing the entries.
    file: File,

    /// The segment capacity.
    cap: u64,

    /// An index holding the offset and length of each entry.
    index: Vec<Entry>,

    /// The last crc value written.
    crc: u32,
}

impl Segment {

    /// Opens the log segment at the specified path.
    pub fn open<P>(file: P) -> Result<Segment> where P: AsRef<Path> {
        let file = try!(OpenOptions::new()
                                    .read(true)
                                    .write(true)
                                    .create(false)
                                    .open(file));
        let cap = try!(file.metadata()).len();
        let mut segment = Segment { file: file, cap: cap, index: Vec::new(), crc: 0};
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
    pub fn create<P>(file: P, capacity: u64) -> Result<Segment> where P: AsRef<Path> {
        if capacity < SEGMENT_HEADER_LEN {
            return Err(Error::new(ErrorKind::InvalidInput, "invalid segment capacity"));
        }

        let mut file = try!(OpenOptions::new()
                                        .read(true)
                                        .write(true)
                                        .create(true)
                                        .open(file));

        let mut header = [0; 8];
        try!((&mut header[..4]).write_all(SEGMENT_HEADER));
        let seed = rand::random();
        LittleEndian::write_u32(&mut header[4..], seed);

        // TODO: this should be using posix_fallocate to avoid creating a sparse file.
        try!(file.set_len(capacity));
        try!(file.write_all(&header));
        try!(file.sync_all());
        Ok(Segment { file: file, cap: capacity, index: Vec::new(), crc: seed })
    }

    pub fn sync(&mut self) ->Result<()> {
        self.file.sync_data()
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
    pub fn append(&mut self, entry: &[u8]) -> Result<Option<usize>> {
        if entry.len() as u64 > self.remaining() {
            return Ok(None);
        }

        let zeros: [u8; 4] = [0; 4];
        let mut buf = [0; 12];

        let entry_offset = self.len();
        try!(self.file.seek(SeekFrom::Start(entry_offset)));

        LittleEndian::write_u64(&mut buf[..8], entry.len() as u64);
        let mut crc = crc32::update(self.crc, &crc32::CASTAGNOLI_TABLE, &buf[..8]);
        LittleEndian::write_u32(&mut buf[8..], crc);

        try!(self.file.write_all(&buf));
        try!(self.file.write_all(entry));
        crc = crc32::update(crc, &crc32::CASTAGNOLI_TABLE, &entry);

        let padding = padding(entry.len());
        if padding > 0 {
            crc = crc32::update(crc, &crc32::CASTAGNOLI_TABLE, &zeros[0..padding]);
            try!((&mut buf[0..padding]).write_all(&zeros[0..padding]));
        }
        LittleEndian::write_u32(&mut buf[padding..padding + 4], crc);
        try!(self.file.write_all(&buf[..padding + 4]));
        self.index.push(Entry { offset: entry_offset + ENTRY_HEADER_LEN,
                                len: entry.len() });
        self.crc = crc;

        Ok(Some(self.index.len() - 1))
    }

    /// Reads the entry at the index from the segment, or `None` if no such
    /// segment exists.
    pub fn read(&mut self, index: usize) -> Result<Take<&mut File>> {
        let Entry { offset, len } = self.index[index];
        try!(self.file.seek(SeekFrom::Start(offset)));
        Ok(Read::by_ref(&mut self.file).take(len as u64))
    }

    /// Returns the capacity of the segment in bytes.
    ///
    /// Each entry is stored with a header and padding, so the entire capacity
    /// will not be available for entry data.
    pub fn capacity(&self) -> u64 {
        self.cap
    }

    /// Number of bytes written to the file.
    pub fn len(&self) -> u64 {
        self.index
            .last()
            .map(|entry| entry.offset + entry.len as u64 + padding(entry.len) as u64 + CRC32_LEN )
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
    pub fn remaining(&self) -> u64 {
        let unpadded = self.cap - self.len() - SEGMENT_HEADER_LEN - CRC32_LEN;
        // Round down to a multiple of four, since padding would otherwise be
        // required.
        unpadded - (unpadded % 4)
    }

    /// Reindex the segment.
    ///
    /// This need not be called in normal circumstances.
    pub fn reindex(&mut self) -> Result<()> {
        self.index.clear();

        try!(self.file.seek(SeekFrom::Start(0)));
        let mut file = BufReader::with_capacity(4096, Read::by_ref(&mut self.file));
        let mut buf = [0; 12];

        try!(read_exact(&mut file, &mut buf[..8]));
        if &buf[..4] != SEGMENT_HEADER {
            return Err(Error::new(ErrorKind::InvalidData, "invalid segment header"));
        }
        self.crc = LittleEndian::read_u32(&buf[4..8]);

        let mut offset = SEGMENT_HEADER_LEN;

        while offset + ENTRY_HEADER_LEN + CRC32_LEN < self.cap {
            try!(read_exact(&mut file, &mut buf));

            let len = LittleEndian::read_u64(&buf[..8]);
            if len > usize::MAX as u64 { break; }
            let padding = padding(len as usize) as u64;
            let padded_len = len + padding;

            let mut crc = crc32::update(self.crc, &crc32::CASTAGNOLI_TABLE, &buf[..8]);
            if crc != LittleEndian::read_u32(&buf[8..]) { break; }
            if offset + ENTRY_HEADER_LEN + padded_len + CRC32_LEN > self.cap { break; }

            let mut pos = 0;
            while pos < padded_len {
                let amt;
                {
                    let buf = try!(file.fill_buf());
                    amt = cmp::min(buf.len(), (padded_len - pos) as usize);
                    crc = crc32::update(crc, &crc32::CASTAGNOLI_TABLE, &buf[..amt]);
                }
                file.consume(amt);
                pos += amt as u64;
            }

            if crc != try!(file.read_u32::<LittleEndian>()) { break; }

            self.index.push(Entry { offset: offset + ENTRY_HEADER_LEN, len: len as usize });
            self.crc = crc;
            offset += ENTRY_HEADER_LEN + padded_len + CRC32_LEN;
        }
        Ok(())
    }
}

/// Returns the number of padding bytes to add to a buffer to ensure 4-byte alignment.
fn padding(len: usize) -> usize {
    0usize.wrapping_sub(len) & 3
}

/// Reads into `buf` until it is full. Returns an error if EOF is encountered first.
pub fn read_exact<R>(read: &mut R, buf: &mut [u8]) -> Result<()>
where R: Read {
    let mut pos = 0;
    let len = buf.len();
    while pos < len {
        let buf1 = &mut buf[pos..];
        match read.read(buf1) {
            Ok(n) => {
                pos += n;
                if n == 0 {
                    return Err(Error::new(ErrorKind::Other, "Premature EOF"))
                }
            },
            Err(ref e) if e.kind() == ErrorKind::Interrupted => (), // retry 
            Err(err) => return Err(err), // retry
        }
    }
    Ok(())
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

    fn test_segment(len: u64) -> (Segment, tempdir::TempDir) {
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

        let mut buf = Vec::new();
        for index in 0..entries.len() {
            buf.clear();
            segment.read(index).unwrap().read_to_end(&mut buf).unwrap();
            assert_eq!(entries[index], &buf[..]);
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

        let mut buf = Vec::new();
        for index in 0..entries.len() {
            buf.clear();
            segment.read(index).unwrap().read_to_end(&mut buf).unwrap();
            assert_eq!(entries[index], &buf[..]);
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
