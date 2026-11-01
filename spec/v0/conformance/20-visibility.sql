-- Visibility (§5). The cases write rows directly: the rule is defined over rows, whichever operation wrote them.

select new_table('gs://lake/t', 0) as t \gset
create function f(id bigint, r int, b bigint, e bigint default 9223372036854775807) returns void language sql as $$
  insert into data_file (table_id, file_id, ref_id, begin_seq, end_seq, path, spec_id, partition, record_count,
                         file_size, footer_size)
  select table_id, id, r, b, e, 'gs://lake/t/' || id || '.parquet', 0, '[]', 1000, 1 << 20, 4096 from tbl;
$$;
create function tomb(id bigint, r int, b bigint, e bigint default 9223372036854775807) returns void language sql as $$
  insert into tombstone select table_id, id, r, b, e from tbl;
$$;

-- case: three refs: what each sees, at the head and in the past
-- rules: VIS-3, VIS-4, VIS-5, VIS-6
-- s1 main adds 1-5. s2 main replaces 5 by 6. s3 b1 forks main at 2. s4 b1 adds 101 and hides 1.
-- s5 b2 forks b1 at 4. s6 b2 adds 201 and hides 2, which it inherits from main through b1.
-- s7 main adds 7 and removes 3. s8 b1 adds 102 and hides 4, after b2 forked.
select f(g, 1, 1) from generate_series(1, 4) g;
select f(5, 1, 1, 2), f(6, 1, 2);
select branch(2, 'b1', 3, 1, 2);
select f(101, 2, 4), tomb(1, 2, 4);
select branch(3, 'b2', 5, 2, 4);
select f(201, 3, 6), tomb(2, 3, 6);
select f(7, 1, 7);
update data_file set end_seq = 7 where file_id = 3;
select f(102, 2, 8), tomb(4, 2, 8);
select expect_files(1, :'t', 8, '{1,2,4,6,7}', 'main: its own changes; no tombstone of a branch');
select expect_files(2, :'t', 8, '{2,3,6,101,102}', 'b1: 3, removed in main after the fork, is still there');
select expect_files(3, :'t', 8, '{3,4,6,101,201}', 'b2: not 102, nor the tombstone of 4, both after its fork');
select expect_files(1, :'t', 1, '{1,2,3,4,5}', 'main at 1');
select expect_files(2, :'t', 4, '{2,3,4,6,101}', 'b1 at 4');
select expect_files(3, :'t', 5, '{2,3,4,6,101}', 'b2 at its creation sees b1 at the fork');

-- case: the chain follows the base valid at each level's sequence, and ends where there is none
-- rules: VIS-3
select branch(2, 'b1', 3, 1, 2);
select branch(3, 'b2', 5, 2, 4);
select expect((select array_agg(array[level, ref_id, seq] order by level) from chain(3, 9)) =
  '{{0,3,9},{1,2,4},{2,1,2}}', 'b2 at 9');
select expect((select array_agg(array[level, ref_id, seq] order by level) from chain(1, 9)) = '{{0,1,9}}',
  'main has no base');
select expect((select array_agg(array[level, ref_id, seq] order by level) from chain(3, 4)) = '{{0,3,4}}',
  'b2 before its creation has no base');
select f(1, 1, 1);
select expect_files(3, :'t', 4, '{}', 'a ref read before its creation sees nothing');

-- case: a chain that reaches itself is refused
-- rules: VIS-3
select branch(2, 'b1', 3, 1, 2);
select branch(3, 'b2', 5, 2, 4);
-- Corrupt on purpose: b1 re-based onto b2, with b2's base valid at the fork.
update ref_base set end_seq = 6 where ref_id = 2;
insert into ref_base values (2, 6, 9223372036854775807, 3, 6);
update ref_base set begin_seq = 1, fork_seq = 1 where ref_id = 3;
select expect_error('select * from chain(2, 7)', 'XX000', 'a cycle');

-- case: a ref has at most one live base, never inherits from the future, and is never its own parent
-- rules: VIS-1
select branch(2, 'b1', 3, 1, 2);
select expect_error('insert into ref_base values (2, 4, 9223372036854775807, 1, 3)', '23505', 'a second live base');
select expect_error('insert into ref_base values (2, 1, 2, 1, 2)', '23514', 'fork_seq after begin_seq');
select expect_error('insert into ref_base values (2, 1, 2, 2, 1)', '23514', 'its own parent');

-- case: a file has at most one live row; a file that changes owner keeps its closed row
-- rules: VIS-7
select f(1, 1, 1);
select branch(2, 'b1', 3, 1, 2);
select expect_error('select f(1, 2, 4)', '23505', 'a second live row of file 1');
select f(2, 2, 4);
update data_file set end_seq = 5 where file_id = 2;
select f(2, 1, 5);
select expect((select count(*) = 2 from data_file where file_id = 2), 'file 2 has two rows, one closed');

-- case: a merge re-bases the branch, and reads of the branch before the merge do not change
-- rules: VIS-2, VIS-7, VIS-8
-- s1 main adds 1, 2. s2 b forks main at 1. s3 b adds 3 and hides 1. s4 main adds 4.
-- s5 b merges into main: 3 moves to main, main closes 1, b's tombstone closes, b is re-based on main at 5.
select f(1, 1, 1), f(2, 1, 1);
select branch(2, 'b', 2, 1, 1);
select f(3, 2, 3), tomb(1, 2, 3);
select f(4, 1, 4);
create temp table before as
  select r, s, array(select file_id from visible_files(r, :'t', s) order by file_id) as files
    from (values (1), (2)) x(r), generate_series(0, 4) s;
update data_file set end_seq = 5 where file_id = 3;
select f(3, 1, 5);
update data_file set end_seq = 5 where file_id = 1;
update tombstone set end_seq = 5 where file_id = 1;
update ref_base set end_seq = 5 where ref_id = 2;
insert into ref_base (ref_id, begin_seq, parent_ref_id, fork_seq) values (2, 5, 1, 5);
select expect(not exists (
  select 1 from before b where b.files <> array(select file_id from visible_files(b.r, :'t', b.s) order by file_id)),
  'every read below 5 returns what it returned before the merge');
select expect_files(2, :'t', 4, '{2,3}', 'b at 4: its own 3, not main''s 4');
select expect_files(1, :'t', 5, '{2,3,4}', 'main after the merge');
select expect_files(2, :'t', 5, '{2,3,4}', 'b after the merge: an empty child of main');

-- case: a grandchild forked before a merge keeps reading through the old base
-- rules: VIS-2, VIS-8
-- As above, plus s4b: c forks b at 3, before the merge at 5.
select f(1, 1, 1), f(2, 1, 1);
select branch(2, 'b', 2, 1, 1);
select f(3, 2, 3), tomb(1, 2, 3);
select branch(3, 'c', 4, 2, 3);
update data_file set end_seq = 5 where file_id = 3;
select f(3, 1, 5);
update data_file set end_seq = 5 where file_id = 1;
update tombstone set end_seq = 5 where file_id = 1;
update ref_base set end_seq = 5 where ref_id = 2;
insert into ref_base (ref_id, begin_seq, parent_ref_id, fork_seq) values (2, 5, 1, 5);
select f(9, 1, 6);
select expect_files(3, :'t', 6, '{2,3}', 'c: b at 3, unaffected by the merge and by main''s 9');

-- case: a tombstone hides the file from its ref and the refs that inherit from it, and from no other
-- rules: VIS-6
-- b1 and b3 fork main at 1; b1 hides main's 1 at 3; b2 forks b1 at 3.
select f(1, 1, 1);
select branch(2, 'b1', 2, 1, 1);
select branch(4, 'b3', 2, 1, 1);
select tomb(1, 2, 3);
select branch(3, 'b2', 4, 2, 3);
select expect_files(1, :'t', 4, '{1}', 'main, the owner');
select expect_files(4, :'t', 4, '{1}', 'b3, a sibling');
select expect_files(2, :'t', 4, '{}', 'b1, the ref of the tombstone');
select expect_files(3, :'t', 4, '{}', 'b2, which inherits it');
select expect_files(2, :'t', 2, '{1}', 'b1 before the tombstone');

-- case: a closed tombstone no longer applies
-- rules: VIS-5
select f(1, 1, 1);
select branch(2, 'b', 2, 1, 1);
select tomb(1, 2, 3, 5);
select expect_files(2, :'t', 4, '{}', 'b at 4, the tombstone is valid');
select expect_files(2, :'t', 5, '{1}', 'b at 5, the tombstone is closed');
