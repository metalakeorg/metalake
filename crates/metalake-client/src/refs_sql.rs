//! A ref's whole life in SQL (I5): the verbs the command line and Python already have, read by the client before
//! DataFusion, as a pipeline's verbs are, and the same in every face (Query, `metalake sql`, `lake.sql`).
//!
//! ```sql
//! CREATE BRANCH [IF NOT EXISTS] dev [FROM main@1234];  CREATE TAG v1;
//! DROP BRANCH [IF EXISTS] dev FORCE;                   DROP TAG v1 FORCE;
//! MERGE BRANCH dev [DRY RUN] [TABLES demo.t, demo.u] [SPLIT] [MESSAGE '…'] [AUTHOR '…'];
//! SHOW BRANCHES;  SHOW TAGS;  SHOW LOG [dev] [LIMIT 20];
//! USE BRANCH dev [AS OF 1234];  USE TAG v1;
//! ALTER BRANCH dev RETAIN 30 DAYS 3 SNAPSHOTS;  ALTER TAG v1 RETAIN 90 DAYS;
//! ALTER LAKE GRACE 7 DAYS;
//! ```
//!
//! Each is two keywords no statement of SQL starts with, so `MERGE INTO`, `USE dev`, `CREATE TABLE` and `ALTER TABLE`
//! stay DataFusion's. A name is a word, folded to lowercase as SQL folds one, or quoted as written: `"feature/x"`. A
//! name is never used again once dropped, so `DROP` says `FORCE`. Each verb answers rows, with the command line's
//! columns: the refs it is about, the merge, the commits, what is kept.

use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::sql::sqlparser::tokenizer::Token;
use metalake::{CommitEntry, Retention};

use crate::merge::{MergeOutcome, MergeRequest};
use crate::refs::{RefInfo, RefKind};
use crate::words::Words;
use crate::{Error, Prepared, Result};

/// A ref's verb, as a statement says it.
#[derive(Debug)]
pub enum RefVerb {
    Create {
        kind: RefKind,
        name: String,
        from: Option<String>,
        if_not_exists: bool,
    },
    Drop {
        kind: RefKind,
        name: String,
        if_exists: bool,
    },
    Merge(MergeRequest),
    Show(RefKind),
    /// The commits of a ref, or of the one the session is on.
    Log {
        on: Option<String>,
        limit: i64,
    },
    Use {
        kind: RefKind,
        name: String,
        /// `AS OF`: a commit sequence, or a time, the session reads the ref at (I7).
        at: Option<String>,
    },
    Retain {
        kind: RefKind,
        name: String,
        retention: Retention,
    },
    Grace(Duration),
}

/// `AS OF 1234` (a commit sequence) or `AS OF '2026-11-04T10:00:00Z'` (a time), if the statement says one.
fn as_of(words: &mut Words) -> Result<Option<String>> {
    if !words.take("AS") {
        return Ok(None);
    }
    if !words.take("OF") {
        return Err(Error::usage("AS OF takes a commit sequence or a time, '…'"));
    }
    match words.peek() {
        Some(Token::Number(..)) => Ok(Some(words.number("AS OF")?.to_string())),
        _ => Ok(Some(words.string("AS OF")?)),
    }
}

/// How many commits `SHOW LOG` answers unless it says.
const LOG: i64 = 20;

fn kind_of(word: Option<&str>) -> Option<RefKind> {
    match word {
        Some("BRANCH") => Some(RefKind::Branch),
        Some("TAG") => Some(RefKind::Tag),
        _ => None,
    }
}

fn kind_name(kind: RefKind) -> &'static str {
    match kind {
        RefKind::Branch => "BRANCH",
        RefKind::Tag => "TAG",
    }
}

/// The ref verb a text says, if it says one: none for every other statement. One that starts as a ref verb and goes on
/// wrongly is refused, saying how it is written.
pub fn ref_verb_of(text: &str) -> Result<Option<RefVerb>> {
    let Some(mut words) = Words::of(text) else {
        return Ok(None);
    };
    let (first, second) = (words.keyword(), words.keyword());
    let verb = match (first.as_deref(), second.as_deref()) {
        (Some("CREATE"), kind @ (Some("BRANCH") | Some("TAG"))) => {
            let kind = kind_of(kind).expect("matched");
            let if_not_exists = words.take("IF");
            if if_not_exists && !(words.take("NOT") && words.take("EXISTS")) {
                return Err(usage_create(kind));
            }
            let name = words.name("a ref")?;
            let from = if words.take("FROM") {
                Some(words.name("the ref it starts from")?)
            } else {
                None
            };
            RefVerb::Create {
                kind,
                name,
                from,
                if_not_exists,
            }
        }
        (Some("DROP"), kind @ (Some("BRANCH") | Some("TAG"))) => {
            let kind = kind_of(kind).expect("matched");
            let if_exists = words.take("IF");
            if if_exists && !words.take("EXISTS") {
                return Err(usage_drop(kind));
            }
            let name = words.name("a ref")?;
            if !words.take("FORCE") {
                return Err(Error::usage(format!(
                    "DROP {} {name} FORCE: a ref's name is never used again, so dropping one says FORCE",
                    kind_name(kind)
                )));
            }
            RefVerb::Drop { kind, name, if_exists }
        }
        (Some("MERGE"), Some("BRANCH")) => RefVerb::Merge(merge(&mut words)?),
        (Some("SHOW"), Some("BRANCHES")) => RefVerb::Show(RefKind::Branch),
        (Some("SHOW"), Some("TAGS")) => RefVerb::Show(RefKind::Tag),
        (Some("SHOW"), Some("LOG")) => {
            let on = match words.peek_keyword().as_deref() {
                Some("LIMIT") => None,
                _ if words.at_end() => None,
                _ => Some(words.name("a ref")?),
            };
            let limit = if words.take("LIMIT") {
                words.number("LIMIT")?
            } else {
                LOG
            };
            RefVerb::Log { on, limit }
        }
        (Some("USE"), kind @ (Some("BRANCH") | Some("TAG"))) => RefVerb::Use {
            kind: kind_of(kind).expect("matched"),
            name: words.name("a ref")?,
            at: as_of(&mut words)?,
        },
        (Some("ALTER"), kind @ (Some("BRANCH") | Some("TAG"))) => {
            let kind = kind_of(kind).expect("matched");
            let name = words.name("a ref")?;
            if !words.take("RETAIN") {
                return Err(usage_retain(kind));
            }
            let mut retention = Retention::default();
            while !words.at_end() {
                let n = words.number("RETAIN")?;
                match words.keyword().as_deref() {
                    Some("DAYS" | "DAY") => retention.max_age = Some(Duration::from_secs(days(n)?)),
                    Some("SNAPSHOTS" | "SNAPSHOT") => {
                        retention.min_snapshots =
                            Some(i32::try_from(n).map_err(|_| Error::usage(format!("{n} snapshots is too many")))?);
                    }
                    _ => return Err(usage_retain(kind)),
                }
            }
            if retention.max_age.is_none() && retention.min_snapshots.is_none() {
                return Err(usage_retain(kind));
            }
            RefVerb::Retain { kind, name, retention }
        }
        (Some("ALTER"), Some("LAKE")) => {
            if !words.take("GRACE") {
                return Err(Error::usage("ALTER LAKE GRACE n DAYS"));
            }
            let n = words.number("GRACE")?;
            if !matches!(words.keyword().as_deref(), Some("DAYS" | "DAY")) {
                return Err(Error::usage("ALTER LAKE GRACE n DAYS"));
            }
            RefVerb::Grace(Duration::from_secs(days(n)?))
        }
        _ => return Ok(None),
    };
    words.end()?;
    Ok(Some(verb))
}

fn days(n: i64) -> Result<u64> {
    u64::try_from(n)
        .ok()
        .and_then(|n| n.checked_mul(86_400))
        .ok_or_else(|| Error::usage(format!("{n} days is not a number of days")))
}

fn usage_create(kind: RefKind) -> Error {
    Error::usage(format!("CREATE {} [IF NOT EXISTS] name [FROM ref]", kind_name(kind)))
}

fn usage_drop(kind: RefKind) -> Error {
    Error::usage(format!("DROP {} [IF EXISTS] name FORCE", kind_name(kind)))
}

fn usage_retain(kind: RefKind) -> Error {
    Error::usage(format!("ALTER {} name RETAIN [n DAYS] [m SNAPSHOTS]", kind_name(kind)))
}

/// `MERGE BRANCH name [DRY RUN] [TABLES a, b] [SPLIT] [MESSAGE '…'] [AUTHOR '…']`, its clauses in any order.
fn merge(words: &mut Words) -> Result<MergeRequest> {
    let mut request = MergeRequest {
        branch: words.name("a branch")?,
        ..MergeRequest::default()
    };
    let usage = || Error::usage("MERGE BRANCH name [DRY RUN] [TABLES a, b] [SPLIT] [MESSAGE '…'] [AUTHOR '…']");
    while !words.at_end() {
        match words.keyword().as_deref() {
            Some("DRY") if words.take("RUN") => request.check = true,
            Some("SPLIT") => request.split = true,
            Some("MESSAGE") => request.message = Some(words.string("MESSAGE")?),
            Some("AUTHOR") => request.author = Some(words.string("AUTHOR")?),
            Some("TABLES" | "TABLE") => loop {
                request.tables.push(words.dotted("a table")?);
                if words.peek() != Some(Token::Comma) {
                    break;
                }
                words.next();
            },
            _ => return Err(usage()),
        }
    }
    Ok(request)
}

/// The refs, as `metalake refs` prints them.
pub(crate) fn ref_rows(refs: &[RefInfo]) -> Result<Prepared> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("kind", DataType::Utf8, false),
        Field::new("parent", DataType::Utf8, true),
        Field::new("fork_seq", DataType::Int64, true),
        Field::new("created_seq", DataType::Int64, false),
        Field::new("protected", DataType::Boolean, false),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from_iter_values(refs.iter().map(|r| r.name.as_str()))),
        Arc::new(StringArray::from_iter_values(refs.iter().map(|r| r.kind.as_str()))),
        Arc::new(StringArray::from_iter(refs.iter().map(|r| r.parent.as_deref()))),
        Arc::new(Int64Array::from_iter(refs.iter().map(|r| r.fork_seq))),
        Arc::new(Int64Array::from_iter_values(refs.iter().map(|r| r.created_seq))),
        Arc::new(BooleanArray::from_iter(refs.iter().map(|r| Some(r.protected)))),
    ];
    batch(schema, columns)
}

/// A merge, as `metalake merge` prints it. The tables merged and rebased are joined by `, `.
pub(crate) fn merge_rows(outcome: &MergeOutcome) -> Result<Prepared> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("branch", DataType::Utf8, false),
        Field::new("outcome", DataType::Utf8, false),
        Field::new("commit", DataType::Int64, true),
        Field::new("merged", DataType::Utf8, false),
        Field::new("rebased", DataType::Utf8, false),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![outcome.branch.as_str()])),
        Arc::new(StringArray::from(vec![outcome.outcome])),
        Arc::new(Int64Array::from(vec![outcome.commit])),
        Arc::new(StringArray::from(vec![outcome.merged.join(", ")])),
        Arc::new(StringArray::from(vec![outcome.rebased.join(", ")])),
    ];
    batch(schema, columns)
}

/// A ref's commits, as `metalake log` prints them.
pub(crate) fn log_rows(commits: &[CommitEntry]) -> Result<Prepared> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("commit_seq", DataType::Int64, false),
        Field::new("committed_at", DataType::Utf8, true),
        Field::new("author", DataType::Utf8, true),
        Field::new("operation", DataType::Utf8, false),
        Field::new("message", DataType::Utf8, true),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(commits.iter().map(|c| c.commit_seq))),
        Arc::new(StringArray::from_iter(
            commits.iter().map(|c| c.committed_at.as_deref()),
        )),
        Arc::new(StringArray::from_iter(commits.iter().map(|c| c.author.as_deref()))),
        Arc::new(StringArray::from_iter_values(
            commits.iter().map(|c| c.operation.as_str()),
        )),
        Arc::new(StringArray::from_iter(commits.iter().map(|c| c.message.as_deref()))),
    ];
    batch(schema, columns)
}

/// What a ref keeps, or the lake's grace, as `metalake retain` prints it.
pub(crate) fn retain_rows(on: Option<&str>, retention: &Retention, grace: Option<Duration>) -> Result<Prepared> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("ref", DataType::Utf8, true),
        Field::new("max_age_seconds", DataType::Int64, true),
        Field::new("min_snapshots", DataType::Int64, true),
        Field::new("grace_seconds", DataType::Int64, true),
    ]));
    let seconds = |d: Option<Duration>| d.map(|d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![on])),
        Arc::new(Int64Array::from(vec![seconds(retention.max_age)])),
        Arc::new(Int64Array::from(vec![retention.min_snapshots.map(i64::from)])),
        Arc::new(Int64Array::from(vec![seconds(grace)])),
    ];
    batch(schema, columns)
}

/// A dropped ref, as `metalake branch delete` prints it: none when `IF EXISTS` found none.
pub(crate) fn dropped_rows(name: Option<&str>) -> Result<Prepared> {
    let schema = Arc::new(Schema::new(vec![Field::new("deleted", DataType::Utf8, false)]));
    let columns: Vec<ArrayRef> = vec![Arc::new(StringArray::from_iter_values(name))];
    batch(schema, columns)
}

fn batch(schema: Arc<Schema>, columns: Vec<ArrayRef>) -> Result<Prepared> {
    let batch = RecordBatch::try_new(schema.clone(), columns).map_err(|e| Error::failed(e.to_string()))?;
    Prepared::from_batches(schema, vec![batch])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn verb(text: &str) -> RefVerb {
        ref_verb_of(text)
            .unwrap()
            .unwrap_or_else(|| panic!("no verb in {text}"))
    }

    #[test]
    fn create_and_drop() {
        assert!(matches!(
            verb("create branch Dev"),
            RefVerb::Create { kind: RefKind::Branch, name, from: None, if_not_exists: false } if name == "dev"
        ));
        assert!(matches!(
            verb("CREATE TAG IF NOT EXISTS \"Release 1\" FROM main@1234;"),
            RefVerb::Create { kind: RefKind::Tag, name, from: Some(from), if_not_exists: true }
                if name == "Release 1" && from == "main@1234"
        ));
        assert!(matches!(
            verb("drop branch if exists \"feature/x\" force"),
            RefVerb::Drop { kind: RefKind::Branch, name, if_exists: true } if name == "feature/x"
        ));
    }

    #[test]
    fn merge_with_its_clauses_in_any_order() {
        let RefVerb::Merge(request) =
            verb("merge branch dev split tables demo.t, u dry run message 'nightly' author 'ana'")
        else {
            panic!()
        };
        assert_eq!(request.branch, "dev");
        assert_eq!(request.tables, ["demo.t", "u"]);
        assert!(request.split && request.check);
        assert_eq!(request.message.as_deref(), Some("nightly"));
        assert_eq!(request.author.as_deref(), Some("ana"));
        let RefVerb::Merge(plain) = verb("MERGE BRANCH dev") else {
            panic!()
        };
        assert!(!plain.check && plain.tables.is_empty());
    }

    #[test]
    fn show_use_retain_grace() {
        assert!(matches!(verb("show branches"), RefVerb::Show(RefKind::Branch)));
        assert!(matches!(verb("SHOW TAGS;"), RefVerb::Show(RefKind::Tag)));
        assert!(matches!(verb("show log"), RefVerb::Log { on: None, limit: LOG }));
        assert!(matches!(verb("show log dev limit 5"), RefVerb::Log { on: Some(on), limit: 5 } if on == "dev"));
        assert!(matches!(verb("show log limit 3"), RefVerb::Log { on: None, limit: 3 }));
        assert!(
            matches!(verb("use branch dev"), RefVerb::Use { kind: RefKind::Branch, name, at: None } if name == "dev")
        );
        assert!(matches!(verb("use tag v1"), RefVerb::Use { kind: RefKind::Tag, .. }));
        assert!(matches!(verb("use branch dev as of 12"), RefVerb::Use { at: Some(at), .. } if at == "12"));
        assert!(matches!(
            verb("use branch main as of '2026-11-04T10:00:00Z'"),
            RefVerb::Use { at: Some(at), .. } if at == "2026-11-04T10:00:00Z"
        ));
        let RefVerb::Retain { retention, .. } = verb("alter branch dev retain 30 days 3 snapshots") else {
            panic!()
        };
        assert_eq!(retention.max_age, Some(Duration::from_secs(30 * 86_400)));
        assert_eq!(retention.min_snapshots, Some(3));
        assert!(matches!(verb("alter lake grace 7 days"), RefVerb::Grace(d) if d.as_secs() == 7 * 86_400));
    }

    #[test]
    fn a_verb_written_wrongly_says_how_it_is_written() {
        for (text, said) in [
            ("drop branch dev", "FORCE"),
            ("create branch if dev", "IF NOT EXISTS"),
            ("alter branch dev retain", "RETAIN"),
            ("alter branch dev retain 3 weeks", "RETAIN"),
            ("alter lake grace", "GRACE"),
            ("merge branch dev now", "MERGE BRANCH"),
            ("show log dev limit many", "LIMIT"),
            ("create branch", "name is missing"),
            ("show branches please", "where the statement ends"),
        ] {
            let error = ref_verb_of(text).expect_err(text);
            assert!(error.message.contains(said), "{text}: {}", error.message);
        }
    }

    #[test]
    fn other_statements_are_not_ref_verbs() {
        for text in [
            "use dev",
            "merge into t using u on t.k = u.k when matched then delete",
            "create table t (k int)",
            "alter table t add column v int",
            "show tables",
            "drop table t",
            "select 1",
            "create pipeline p as insert into t select 1",
        ] {
            assert!(ref_verb_of(text).unwrap().is_none(), "{text}");
        }
    }
}
