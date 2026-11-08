//! Which subtask of a source reads a task: the one its file hashes to. Every task of a file goes to it, so that a file's
//! history — its rows read or added, the vectors that delete some, its removal — reaches one subtask in commit order,
//! and no row is retracted before it's emitted. Files moved by a compaction may go elsewhere; what follows a source is
//! order independent for that (updating aggregates), or refuses updating input (windows and joins).

/// Whether subtask `index` of `parallelism` reads the tasks of file `file_id`.
pub fn reads(file_id: i64, index: usize, parallelism: usize) -> bool {
    assert!(index < parallelism, "subtask {index} of {parallelism}");
    mix(file_id as u64) % parallelism as u64 == index as u64
}

/// SplitMix64's finalizer. `file_id`s come from one sequence for all of a catalog's tables, so one table's can be
/// strided, and a plain modulo would send them all to a few subtasks; the hash also has to be the same on every worker
/// and after a restore, which `std`'s hashers don't promise across Rust versions.
fn mix(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn subtask(file_id: i64, parallelism: usize) -> usize {
        let readers: Vec<usize> = (0..parallelism).filter(|i| reads(file_id, *i, parallelism)).collect();
        assert_eq!(readers.len(), 1, "file {file_id} of {parallelism}: {readers:?}");
        readers[0]
    }

    #[test]
    fn each_file_has_one_reader() {
        for parallelism in 1..=16 {
            for file_id in (-50..2_000).chain([i64::MIN, i64::MAX]) {
                subtask(file_id, parallelism);
            }
        }
    }

    #[test]
    fn strided_files_spread_evenly() {
        for (stride, parallelism) in [(2, 2), (2, 4), (7, 7), (16, 8), (3, 5)] {
            let mut counts = vec![0usize; parallelism];
            for k in 0..10_000 {
                counts[subtask(1 + stride * k, parallelism)] += 1;
            }
            let even = 10_000 / parallelism;
            for c in &counts {
                assert!(
                    c.abs_diff(even) * 4 <= even,
                    "stride {stride} over {parallelism}: {counts:?}"
                );
            }
        }
    }

    /// A restore finds each file where it was read before: these must never change.
    #[test]
    fn the_hash_is_fixed() {
        for (file_id, parallelism, index) in [
            (1, 4, 1),
            (2, 4, 2),
            (3, 4, 0),
            (42, 7, 3),
            (1_000_003, 16, 3),
            (-5, 3, 0),
        ] {
            assert_eq!(subtask(file_id, parallelism), index, "file {file_id} of {parallelism}");
        }
    }
}
