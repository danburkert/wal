//! Simulates a process crash while writing to a write ahead log segment.
//!
//! Steps:
//!
//! 1. The test process creates a seed and a temporary directory
//! 2. The test process creates a subprocess with the seed and directory as
//!    arguments.
//! 3. The subprocess creates a segment in the directory, and appends entries to
//!    the segment based on the seed.
//! 4. The subprocess aborts without closing resources or cleaning up state.
//! 5. The test process reads the entries from the segment, ensuring that no
//!    corruption or data loss has occured.

extern crate rand;
extern crate tempdir;
extern crate wal;

use std::env;
use std::process;
use std::collections::HashMap;

use rand::{Rng, SeedableRng};

use wal::Segment;

const MAX_ENTRY_LEN: u32 = 16 * 1024;
const MAX_ENTRIES: u32 = 2048;
// Add some extra space for segment overhead.
const SEGMENT_CAPACITY: u32 = MAX_ENTRY_LEN * MAX_ENTRIES * 4 / 3;
const EXIT_CODE: i32 = 42;

#[test]
fn process_crash() {
    let vars: HashMap<String, String> = env::vars().collect();
    if vars.contains_key("SEED") && vars.contains_key("SEGMENT_PATH") {
        let seed: usize = vars["SEED"].parse().unwrap();
        let path = vars["SEGMENT_PATH"].clone();
        subprocess(path, seed);
    } else {
        test()
    }
}

fn test() {
    let tempdir = tempdir::TempDir::new("process-crash").unwrap();
    let seed: usize = rand::thread_rng().next_u32() as usize;
    let path = tempdir.path().join("segment");

    println!("Spawning subprocess; path: {:?}; seed: {}", path, seed);

    let exit_code = process::Command::new(env::current_exe().unwrap())
                                     .env("SEED", format!("{}", seed))
                                     .env("SEGMENT_PATH", format!("{}", path.display()))
                                     .status()
                                     .unwrap()
                                     .code()
                                     .unwrap();

    assert_eq!(EXIT_CODE, exit_code);

    let segment = wal::SyncSegment::open(path).unwrap();

    for (i, entry) in EntryGenerator::new(seed).enumerate() {
        assert_eq!(&entry[..], segment.entry(i).unwrap());
    }
}

fn subprocess(path: String, seed: usize) {
    let mut segment = wal::SyncSegment::create(path, SEGMENT_CAPACITY as usize).unwrap();

    for entry in EntryGenerator::new(seed) {
        segment.append(&entry).expect("segment capacity exhausted");
    }

    process::exit(EXIT_CODE);
}

struct EntryGenerator {
    rng: rand::StdRng,
    entries_remaining: u32,
}

impl EntryGenerator {
    fn new(seed: usize) -> EntryGenerator {
        let mut rng = rand::StdRng::from_seed(&[seed]);
        let num_entries = rng.next_u32() % MAX_ENTRIES;
        EntryGenerator {
            rng: rng,
            entries_remaining: num_entries,
        }
    }
}

impl Iterator for EntryGenerator {
    type Item = Vec<u8>;
    fn next(&mut self) -> Option<Vec<u8>> {
        if self.entries_remaining > 0 {
            self.entries_remaining -= 1;
            let len = self.rng.next_u32() % MAX_ENTRY_LEN;
            Some(self.rng.gen_iter().take(len as usize).collect())
        } else {
            None
        }
    }
}
