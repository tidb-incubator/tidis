use super::*;
use std::collections::VecDeque;
use std::sync::Mutex as StdMutex;
use std::time::Duration;

#[derive(Default)]
struct Calls {
    begins: Vec<Instant>,
    operations: usize,
    commits: usize,
    rollbacks: usize,
}

struct Attempt {
    begin: TiKVResult<()>,
    operation: AsyncResult<()>,
    commit: TiKVResult<()>,
    rollback: TiKVResult<()>,
}

impl Default for Attempt {
    fn default() -> Self {
        Self {
            begin: Ok(()),
            operation: Ok(()),
            commit: Ok(()),
            rollback: Ok(()),
        }
    }
}

struct MockTxn {
    attempt: Attempt,
    calls: Arc<StdMutex<Calls>>,
}

impl TransactionLifecycle for MockTxn {
    fn commit(&mut self) -> BoxFuture<'_, TiKVResult<()>> {
        self.calls.lock().unwrap().commits += 1;
        Box::pin(futures::future::ready(std::mem::replace(
            &mut self.attempt.commit,
            Ok(()),
        )))
    }

    fn rollback(&mut self) -> BoxFuture<'_, TiKVResult<()>> {
        self.calls.lock().unwrap().rollbacks += 1;
        Box::pin(futures::future::ready(std::mem::replace(
            &mut self.attempt.rollback,
            Ok(()),
        )))
    }
}

struct Fixture {
    max_attempts: u32,
    attempts: VecDeque<Attempt>,
    calls: Arc<StdMutex<Calls>>,
}

impl Fixture {
    fn new(max_attempts: u32, attempts: Vec<Attempt>) -> Self {
        Self {
            max_attempts,
            attempts: attempts.into(),
            calls: Arc::new(StdMutex::new(Calls::default())),
        }
    }

    async fn exec(&mut self) -> AsyncResult<()> {
        let attempts = &mut self.attempts;
        let calls = &self.calls;
        exec_in_new_txn(
            self.max_attempts,
            || {
                calls.lock().unwrap().begins.push(Instant::now());
                let mut attempt = attempts.pop_front().expect("unexpected transaction replay");
                let result = std::mem::replace(&mut attempt.begin, Ok(())).map(|_| MockTxn {
                    attempt,
                    calls: calls.clone(),
                });
                futures::future::ready(result)
            },
            |txn| {
                Box::pin(async move {
                    let mut txn = txn.lock().await;
                    txn.calls.lock().unwrap().operations += 1;
                    std::mem::replace(&mut txn.attempt.operation, Ok(()))
                })
            },
        )
        .await
    }
}

fn conflict() -> Error {
    let mut err = Error::KeyError(Default::default());
    if let Error::KeyError(ref mut key) = err {
        key.conflict = Some(Default::default());
    }
    // This is how the pinned client's multi-region request plan wraps conflicts.
    Error::ExtractedErrors(vec![Error::MultipleKeyErrors(vec![err])])
}

fn undetermined() -> Error {
    Error::UndeterminedError(Box::new(StringError("primary response lost".into())))
}

fn assert_original(result: AsyncResult<()>, expected: &str) {
    assert_eq!(format!("{:?}", result.unwrap_err()), expected);
}

#[tokio::test]
async fn terminal_and_undetermined_commits_are_never_replayed_or_rolled_back() {
    let mut aborted = Error::KeyError(Default::default());
    if let Error::KeyError(ref mut key) = aborted {
        key.abort = "transaction aborted".into();
    }
    for err in [
        undetermined(),
        StringError("terminal commit error".into()),
        Error::MultipleKeyErrors(vec![aborted]),
        Error::MultipleKeyErrors(vec![undetermined()]),
        Error::ExtractedErrors(vec![conflict(), undetermined()]),
        Error::PessimisticLockError {
            inner: Box::new(undetermined()),
            success_keys: vec![],
        },
    ] {
        let expected = format!("TikvClient({:?})", err);
        // Any replay also fails because the queue contains only one attempt.
        let mut fixture = Fixture::new(
            3,
            vec![Attempt {
                commit: Err(err),
                ..Attempt::default()
            }],
        );
        assert_original(fixture.exec().await, &expected);
        let calls = fixture.calls.lock().unwrap();
        assert_eq!(
            (
                calls.begins.len(),
                calls.operations,
                calls.commits,
                calls.rollbacks
            ),
            (1, 1, 1, 0)
        );
    }
}

#[tokio::test]
async fn conflicts_exhaust_the_budget_with_backoff_and_preserve_the_last_error() {
    let expected = format!("{:?}", RTError::from(conflict()));
    let mut fixture = Fixture::new(
        3,
        (0..3)
            .map(|_| Attempt {
                commit: Err(conflict()),
                ..Attempt::default()
            })
            .collect(),
    );
    assert_original(fixture.exec().await, &expected);
    let calls = fixture.calls.lock().unwrap();
    assert_eq!(
        (
            calls.begins.len(),
            calls.operations,
            calls.commits,
            calls.rollbacks
        ),
        (3, 3, 3, 3)
    );
    let begins = calls.begins.clone();
    drop(calls);
    for (i, pair) in begins.windows(2).enumerate() {
        assert!(pair[1] - pair[0] >= Duration::from_millis(12 + 10 * i as u64));
    }
    assert_eq!(txn_retry_delay_ms(20), 200);
    assert_eq!(txn_retry_delay_ms(u32::MAX), 200);
}

#[tokio::test]
async fn sequential_calls_each_get_a_fresh_budget() {
    let mut fixture = Fixture::new(
        2,
        vec![
            Attempt {
                commit: Err(conflict()),
                ..Attempt::default()
            },
            Attempt::default(),
            Attempt {
                operation: Err(RTError::from(conflict())),
                ..Attempt::default()
            },
            Attempt::default(),
        ],
    );
    fixture.exec().await.unwrap();
    fixture.exec().await.unwrap();
    let calls = fixture.calls.lock().unwrap();
    assert_eq!(
        (
            calls.begins.len(),
            calls.operations,
            calls.commits,
            calls.rollbacks
        ),
        (4, 4, 3, 2)
    );
    assert!(calls.begins[3] - calls.begins[2] >= Duration::from_millis(12));
}

#[tokio::test]
async fn zero_attempts_rejects_without_starting_a_transaction() {
    let mut fixture = Fixture::new(0, vec![]);
    assert_eq!(
        fixture.exec().await.unwrap_err().to_string(),
        "txn_retry_count must be greater than zero"
    );
    assert!(fixture.calls.lock().unwrap().begins.is_empty());
}

#[tokio::test]
async fn rollback_failure_preserves_operation_and_commit_errors_without_replay() {
    for failed_commit in [false, true] {
        let original = RTError::from(conflict());
        let expected = format!("{:?}", original);
        let mut attempt = Attempt {
            rollback: Err(StringError("cleanup failed".into())),
            ..Attempt::default()
        };
        if failed_commit {
            attempt.commit = Err(conflict());
        } else {
            attempt.operation = Err(original);
        }
        let mut fixture = Fixture::new(3, vec![attempt]);
        assert_original(fixture.exec().await, &expected);
        let calls = fixture.calls.lock().unwrap();
        assert_eq!(
            (calls.begins.len(), calls.operations, calls.rollbacks),
            (1, 1, 1)
        );
    }
}

#[tokio::test]
async fn terminal_operation_error_is_rolled_back_and_returned() {
    let mut fixture = Fixture::new(
        3,
        vec![Attempt {
            operation: Err(RTError::String("invalid operation")),
            ..Attempt::default()
        }],
    );
    assert!(matches!(
        fixture.exec().await,
        Err(RTError::String("invalid operation"))
    ));
    let calls = fixture.calls.lock().unwrap();
    assert_eq!(
        (calls.begins.len(), calls.commits, calls.rollbacks),
        (1, 0, 1)
    );
}

#[tokio::test]
async fn begin_failures_back_off_and_terminal_errors_stop_early() {
    let mut fixture = Fixture::new(
        3,
        vec![
            Attempt {
                begin: Err(Error::EntryNotFoundInRegionCache),
                ..Attempt::default()
            },
            Attempt {
                begin: Err(StringError("PD unavailable".into())),
                ..Attempt::default()
            },
        ],
    );
    assert_original(
        fixture.exec().await,
        &format!("{:?}", RTError::from(StringError("PD unavailable".into()))),
    );
    let calls = fixture.calls.lock().unwrap();
    assert_eq!(
        (calls.begins.len(), calls.operations, calls.rollbacks),
        (2, 0, 0)
    );
    assert!(calls.begins[1] - calls.begins[0] >= Duration::from_millis(12));
}
