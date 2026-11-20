//! Public policy contract under deterministic, adversarial acquisition/dispatch races.

use std::{
    future::{poll_fn, ready},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};

use neotales_dbapi::{
    BufferedReader, Cancellation, Capabilities, DbError, DbEvent, DbFuture, DbType, Delay,
    ErrorKind, Parameter, PolicyPool, Pool, PoolPolicy, QueryOptions, QuerySetting, Reader,
    RetryPolicy, RetryRuntime, RowLimit, Session, SqlCaptureHook, TelemetryOutcome, Transaction,
    TxOptions, Value,
};

#[derive(Default)]
struct Gate {
    ready: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

impl Gate {
    fn poll(&self, cx: &Context<'_>) -> Poll<()> {
        let mut waker = self.waker.lock().unwrap();
        if self.ready.load(Ordering::SeqCst) {
            *waker = None;
            Poll::Ready(())
        } else {
            *waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }

    fn open(&self) {
        self.ready.store(true, Ordering::SeqCst);
        let waker = self.waker.lock().unwrap().take();
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

#[derive(Clone, Copy, Default)]
enum Interrupt {
    #[default]
    None,
    Cancel,
    Deadline,
}

#[derive(Default)]
struct State {
    received: Mutex<Vec<ReceivedOptions>>,
    acquire: Gate,
    statement: Gate,
    deadline: Gate,
    cancellation: Arc<Cancellation>,
    acquisitions: AtomicUsize,
    dispatches: AtomicUsize,
    session_drops: AtomicUsize,
    factories: AtomicUsize,
    sleeps: AtomicUsize,
    acquire_effect: Mutex<Interrupt>,
    completion_effect: Mutex<Interrupt>,
    error: Mutex<Option<ErrorKind>>,
    timer_opens_acquire: AtomicBool,
    timer_cancels_after_acquire: AtomicBool,
}

type ReceivedOptions = (Vec<Parameter>, Option<u64>, Option<RowLimit>);

impl State {
    fn apply(&self, interrupt: Interrupt) {
        match interrupt {
            Interrupt::None => {}
            Interrupt::Cancel => self.cancellation.cancel(),
            Interrupt::Deadline => self.deadline.open(),
        }
    }

    fn runtime(self: &Arc<Self>) -> RetryRuntime {
        let timer_state = self.clone();
        let sleep_state = self.clone();
        RetryRuntime::new(
            self.cancellation.clone(),
            move || {
                assert_eq!(timer_state.acquisitions.load(Ordering::SeqCst), 0);
                assert_eq!(timer_state.factories.fetch_add(1, Ordering::SeqCst), 0);
                let state = timer_state.clone();
                let mut completed = false;
                Box::pin(poll_fn(move |cx| {
                    assert!(!completed, "the completed timer must not be polled again");
                    if state.timer_opens_acquire.load(Ordering::SeqCst)
                        && state.acquisitions.load(Ordering::SeqCst) > 0
                    {
                        state.acquire.open();
                        state.deadline.open();
                    }
                    if state.timer_cancels_after_acquire.load(Ordering::SeqCst)
                        && state.acquisitions.load(Ordering::SeqCst) > 0
                    {
                        state.cancellation.cancel();
                    }
                    let result = state.deadline.poll(cx);
                    completed = result.is_ready();
                    result
                }))
            },
            move |_| {
                sleep_state.sleeps.fetch_add(1, Ordering::SeqCst);
                Box::pin(ready(()))
            },
            || 0,
        )
    }

    fn pool(self: &Arc<Self>, attempts: u32) -> PolicyPool {
        PolicyPool::new(
            FakePool(self.clone()),
            PoolPolicy {
                retry: RetryPolicy::new(attempts, Delay::Fixed(Duration::ZERO), true).unwrap(),
                retry_runtime: Some(self.runtime()),
                ..PoolPolicy::default()
            },
        )
    }

    fn assert_stopped(&self, dispatches: usize) {
        assert_eq!(self.factories.load(Ordering::SeqCst), 1);
        assert_eq!(self.acquisitions.load(Ordering::SeqCst), 1);
        assert_eq!(self.dispatches.load(Ordering::SeqCst), dispatches);
        assert_eq!(self.session_drops.load(Ordering::SeqCst), 1);
        assert_eq!(self.sleeps.load(Ordering::SeqCst), 0);
    }
}

struct FakePool(Arc<State>);
struct FakeSession(Arc<State>);

impl Pool for FakePool {
    fn capabilities(&self) -> Capabilities {
        Capabilities::default()
    }

    fn acquire_async(&self) -> DbFuture<'_, Box<dyn Session>> {
        Box::pin(async {
            assert_eq!(self.0.factories.load(Ordering::SeqCst), 1);
            self.0.acquisitions.fetch_add(1, Ordering::SeqCst);
            self.0.apply(*self.0.acquire_effect.lock().unwrap());
            poll_fn(|cx| self.0.acquire.poll(cx)).await;
            Ok(Box::new(FakeSession(self.0.clone())) as Box<dyn Session>)
        })
    }
}

impl FakeSession {
    async fn dispatch(&self) -> Result<(), DbError> {
        self.0.dispatches.fetch_add(1, Ordering::SeqCst);
        poll_fn(|cx| self.0.statement.poll(cx)).await;
        self.0.apply(*self.0.completion_effect.lock().unwrap());
        self.0
            .error
            .lock()
            .unwrap()
            .map_or(Ok(()), |kind| Err(DbError::new(kind)))
    }
}

impl Drop for FakeSession {
    fn drop(&mut self) {
        self.0.session_drops.fetch_add(1, Ordering::SeqCst);
    }
}

impl Session for FakeSession {
    fn execute_async<'a>(&'a mut self, _: &'a str, options: QueryOptions) -> DbFuture<'a, u64> {
        self.0.received.lock().unwrap().push((
            options.parameters.clone(),
            options.provider_execute_options().timeout_ms,
            None,
        ));
        Box::pin(async { self.dispatch().await.map(|()| 1) })
    }

    fn fetch_async<'a>(
        &'a mut self,
        _: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, Box<dyn Reader + 'a>> {
        self.0.received.lock().unwrap().push((
            options.parameters.clone(),
            options.provider_execute_options().timeout_ms,
            Some(options.resolved_limit(neotales_dbapi::PoolDefaults::default())),
        ));
        Box::pin(async {
            self.dispatch()
                .await
                .map(|()| Box::new(BufferedReader::new(Vec::new())) as Box<dyn Reader>)
        })
    }

    fn execute_trusted_script_async<'a>(
        &'a mut self,
        _: &'a str,
        _: QueryOptions,
    ) -> DbFuture<'a, ()> {
        Box::pin(ready(Err(DbError::new(ErrorKind::Unsupported))))
    }

    fn begin_async<'a>(&'a mut self, _: TxOptions) -> DbFuture<'a, Box<dyn Transaction<'a> + 'a>> {
        Box::pin(ready(Err(DbError::new(ErrorKind::Unsupported))))
    }
}

#[test]
fn pool_preserves_parameters_and_resolves_timeout_and_limit() {
    for fetch in [false, true] {
        for override_defaults in [false, true] {
            let state = Arc::new(State::default());
            state.acquire.open();
            state.statement.open();
            let pool = state.pool(3);
            let parameter = Parameter::new(DbType::I64, Value::I64(42)).unwrap();
            let limit = RowLimit::new(2, 128).unwrap();
            let mut query = options().bind(parameter.clone());
            if override_defaults {
                query = query.timeout(Duration::from_millis(17));
                if fetch {
                    query = query.limit(limit);
                }
            }
            let mut future: DbFuture<'_, ()> = if fetch {
                Box::pin(async { pool.fetch_async("SELECT ?", query).await.map(|_| ()) })
            } else {
                Box::pin(async { pool.execute_async("SELECT ?", query).await.map(|_| ()) })
            };
            assert_eq!(
                future
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop())),
                Poll::Ready(Ok(()))
            );
            let received = state.received.lock().unwrap();
            assert_eq!(received[0].0, vec![parameter]);
            assert_eq!(received[0].1, override_defaults.then_some(17));
            if fetch {
                assert_eq!(
                    received[0].2,
                    Some(if override_defaults {
                        limit
                    } else {
                        RowLimit::default()
                    })
                );
            }
        }
    }
}

#[test]
fn bare_pool_rejects_unhandled_telemetry_instead_of_discarding_it() {
    let state = Arc::new(State::default());
    let pool = FakePool(state.clone());
    let mut future =
        pool.execute_async("SELECT 1", QueryOptions::new().telemetry(QuerySetting::On));
    assert_eq!(
        future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Err(DbError::new(ErrorKind::Unsupported)))
    );
    assert_eq!(state.acquisitions.load(Ordering::SeqCst), 0);
}

fn operation(pool: &dyn Pool, fetch: bool, options: QueryOptions) -> DbFuture<'_, ()> {
    const SQL: &str = "INSERT INTO secrets VALUES ('private literal') RETURNING value";
    Box::pin(async move {
        if fetch {
            pool.fetch_async(SQL, options.limit(RowLimit::new(1, 64).unwrap()))
                .await
                .map(|_| ())
        } else {
            pool.execute_async(SQL, options).await.map(|_| ())
        }
    })
}

fn options() -> QueryOptions {
    QueryOptions::new().replay_safe()
}

#[derive(Default)]
struct WakeFlag(AtomicBool);
impl Wake for WakeFlag {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[test]
fn timer_is_created_once_before_acquisition_even_on_immediate_success() {
    for fetch in [false, true] {
        let state = Arc::new(State::default());
        state.acquire.open();
        state.statement.open();
        let pool = state.pool(3);
        let mut work = operation(&pool, fetch, options());
        assert_eq!(
            work.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Ok(()))
        );
        state.assert_stopped(1);
    }
}

#[test]
fn synchronous_acquisition_cannot_dispatch_after_expiry_or_cancellation() {
    for fetch in [false, true] {
        for interrupt in [Interrupt::Cancel, Interrupt::Deadline] {
            let state = Arc::new(State::default());
            state.acquire.open();
            state.statement.open();
            *state.acquire_effect.lock().unwrap() = interrupt;
            let pool = state.pool(3);
            let mut work = operation(&pool, fetch, options());
            assert_eq!(
                work.as_mut().poll(&mut Context::from_waker(Waker::noop())),
                Poll::Ready(Err(DbError::new(match interrupt {
                    Interrupt::Cancel => ErrorKind::Cancelled,
                    _ => ErrorKind::Timeout,
                })))
            );
            state.assert_stopped(0);
        }
    }
}

#[test]
fn simultaneous_acquisition_deadline_and_cancellation_never_dispatch() {
    for fetch in [false, true] {
        for cancel in [false, true] {
            let state = Arc::new(State::default());
            state.statement.open();
            let pool = state.pool(3);
            let mut work = operation(&pool, fetch, options());
            let wake = Arc::new(WakeFlag::default());
            let waker = Waker::from(wake.clone());
            let mut cx = Context::from_waker(&waker);
            assert!(work.as_mut().poll(&mut cx).is_pending());
            assert_eq!(state.dispatches.load(Ordering::SeqCst), 0);
            state.acquire.open();
            state.deadline.open();
            if cancel {
                state.cancellation.cancel();
            }
            assert!(wake.0.load(Ordering::SeqCst));
            assert_eq!(
                work.as_mut().poll(&mut cx),
                Poll::Ready(Err(DbError::new(if cancel {
                    ErrorKind::Cancelled
                } else {
                    ErrorKind::Timeout
                })))
            );
            state.assert_stopped(0);
        }
    }
}

#[test]
fn timer_success_probe_must_not_advance_acquisition_into_sql_dispatch() {
    for fetch in [false, true] {
        let state = Arc::new(State::default());
        state.statement.open();
        state.timer_opens_acquire.store(true, Ordering::SeqCst);
        let pool = state.pool(3);
        let mut work = operation(&pool, fetch, options());
        assert_eq!(
            work.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Err(DbError::new(ErrorKind::Timeout)))
        );
        state.assert_stopped(0);
    }
}

#[test]
fn cancellation_becoming_ready_while_polling_dispatch_timer_prevents_sql() {
    for fetch in [false, true] {
        let state = Arc::new(State::default());
        state.acquire.open();
        state.statement.open();
        state
            .timer_cancels_after_acquire
            .store(true, Ordering::SeqCst);
        let pool = state.pool(3);
        let mut work = operation(&pool, fetch, options());
        assert_eq!(
            work.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Err(DbError::new(ErrorKind::Cancelled)))
        );
        state.assert_stopped(0);
    }
}

// The cancellation listener owns the last old waker when the engine registers a new one.
// Its destruction makes acquisition, deadline, and cancellation ready in that exact window.
struct AcquireOnDrop(Arc<State>);
#[allow(clippy::manual_noop_waker)] // The destructor, not wake(), drives the race.
impl Wake for AcquireOnDrop {
    fn wake(self: Arc<Self>) {}
}
impl Drop for AcquireOnDrop {
    fn drop(&mut self) {
        self.0.acquire.open();
        self.0.deadline.open();
        self.0.cancellation.cancel();
    }
}

#[test]
fn waker_replacement_cannot_probe_acquisition_into_dispatch() {
    for fetch in [false, true] {
        let state = Arc::new(State::default());
        state.statement.open();
        let pool = state.pool(3);
        let mut work = operation(&pool, fetch, options());
        let waker = Waker::from(Arc::new(AcquireOnDrop(state.clone())));
        assert!(
            work.as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        state.acquire.waker.lock().unwrap().take();
        state.deadline.waker.lock().unwrap().take();
        drop(waker);
        assert!(!state.cancellation.is_cancelled());
        assert_eq!(
            work.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Err(DbError::new(ErrorKind::Cancelled)))
        );
        state.assert_stopped(0);
    }
}

#[test]
fn already_dispatched_success_wins_but_pending_fetch_is_not_assumed_read_only() {
    for fetch in [false, true] {
        for interrupt in [Interrupt::Cancel, Interrupt::Deadline] {
            for success in [false, true] {
                let state = Arc::new(State::default());
                state.acquire.open();
                let pool = state.pool(3);
                let mut work = operation(&pool, fetch, options());
                let mut cx = Context::from_waker(Waker::noop());
                assert!(work.as_mut().poll(&mut cx).is_pending());
                assert_eq!(state.dispatches.load(Ordering::SeqCst), 1);
                if success {
                    state.statement.open();
                }
                state.apply(interrupt);
                assert_eq!(
                    work.as_mut().poll(&mut cx),
                    Poll::Ready(if success {
                        Ok(())
                    } else {
                        Err(DbError::new(ErrorKind::WriteOutcomeUnknown))
                    })
                );
                state.assert_stopped(1);
            }
        }
    }
}

#[test]
fn authoritative_unknown_outcomes_survive_interruptions_on_both_statement_paths() {
    for fetch in [false, true] {
        for kind in [
            ErrorKind::WriteOutcomeUnknown,
            ErrorKind::CommitOutcomeUnknown,
        ] {
            for interrupt in [Interrupt::None, Interrupt::Cancel, Interrupt::Deadline] {
                for attempts in [1, 3] {
                    for delayed in [false, true] {
                        let state = Arc::new(State::default());
                        state.acquire.open();
                        *state.error.lock().unwrap() = Some(kind);
                        *state.completion_effect.lock().unwrap() = interrupt;
                        let pool = state.pool(attempts);
                        let mut work = operation(&pool, fetch, options());
                        let mut cx = Context::from_waker(Waker::noop());
                        if delayed {
                            assert!(work.as_mut().poll(&mut cx).is_pending());
                            // Make the explicit provider error ready in the engine's success probe.
                            state.apply(interrupt);
                        }
                        state.statement.open();
                        assert_eq!(
                            work.as_mut().poll(&mut cx),
                            Poll::Ready(Err(DbError::new(kind)))
                        );
                        state.assert_stopped(1);
                    }
                }
            }
        }
    }
}

#[test]
fn nested_policy_keeps_unknown_outcomes_and_capture_off_without_inner_retries() {
    for fetch in [false, true] {
        for kind in [
            ErrorKind::WriteOutcomeUnknown,
            ErrorKind::CommitOutcomeUnknown,
        ] {
            let state = Arc::new(State::default());
            state.acquire.open();
            state.statement.open();
            *state.error.lock().unwrap() = Some(kind);
            *state.completion_effect.lock().unwrap() = Interrupt::Cancel;
            let events = Arc::new(Mutex::new(Vec::<DbEvent>::new()));
            let log = events.clone();
            let hook_calls = Arc::new(AtomicUsize::new(0));
            let calls = hook_calls.clone();
            let policy = PoolPolicy {
                telemetry_enabled: true,
                sql_capture_enabled: true,
                sql_hook: Some(Arc::new(move |_| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Some("hook must not run".to_owned())
                })),
                observer: Some(Arc::new(move |event| {
                    log.lock().unwrap().push(event.clone());
                })),
                retry: RetryPolicy::new(3, Delay::Fixed(Duration::ZERO), true).unwrap(),
                retry_runtime: Some(state.runtime()),
                ..PoolPolicy::default()
            };
            let inner = PolicyPool::new(FakePool(state.clone()), policy.clone());
            let outer = PolicyPool::new(inner, policy);
            let mut work = operation(&outer, fetch, options().sql_capture(QuerySetting::Off));
            assert_eq!(
                work.as_mut().poll(&mut Context::from_waker(Waker::noop())),
                Poll::Ready(Err(DbError::new(kind)))
            );
            state.assert_stopped(1);
            assert_eq!(hook_calls.load(Ordering::SeqCst), 0);
            let events = events.lock().unwrap();
            assert_eq!(events.len(), 2);
            assert!(
                events
                    .iter()
                    .all(|event| event.sql.is_none()
                        && event.outcome == TelemetryOutcome::Error(kind))
            );
        }
    }
}

#[test]
fn expired_budget_at_retry_checkpoint_never_starts_another_acquisition() {
    for fetch in [false, true] {
        let state = Arc::new(State::default());
        state.acquire.open();
        state.statement.open();
        *state.error.lock().unwrap() = Some(ErrorKind::Busy);
        let pool = state.pool(3);
        let mut work = operation(&pool, fetch, options());
        let mut cx = Context::from_waker(Waker::noop());
        assert!(work.as_mut().poll(&mut cx).is_pending());
        assert_eq!(state.sleeps.load(Ordering::SeqCst), 1);
        state.deadline.open();
        assert_eq!(
            work.as_mut().poll(&mut cx),
            Poll::Ready(Err(DbError::new(ErrorKind::Timeout)))
        );
        assert_eq!(state.factories.load(Ordering::SeqCst), 1);
        assert_eq!(state.acquisitions.load(Ordering::SeqCst), 1);
        assert_eq!(state.dispatches.load(Ordering::SeqCst), 1);
        assert_eq!(state.session_drops.load(Ordering::SeqCst), 1);
        assert_eq!(state.sleeps.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn sql_hook_expiry_after_acquisition_is_checked_before_dispatch() {
    for fetch in [false, true] {
        let state = Arc::new(State::default());
        state.acquire.open();
        state.statement.open();
        let hook_state = state.clone();
        let pool = PolicyPool::new(
            FakePool(state.clone()),
            PoolPolicy {
                telemetry_enabled: true,
                sql_capture_enabled: true,
                sql_hook: Some(Arc::new(move |_| {
                    hook_state.deadline.open();
                    None
                })),
                observer: Some(Arc::new(|_| {})),
                retry: RetryPolicy::new(3, Delay::Fixed(Duration::ZERO), true).unwrap(),
                retry_runtime: Some(state.runtime()),
                ..PoolPolicy::default()
            },
        );
        let mut work = operation(&pool, fetch, options());
        assert_eq!(
            work.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Err(DbError::new(ErrorKind::Timeout)))
        );
        state.assert_stopped(0);
    }
}

fn nested_pool(
    state: &Arc<State>,
    layers: usize,
    hook: SqlCaptureHook,
    events: &Arc<Mutex<Vec<DbEvent>>>,
) -> Arc<dyn Pool> {
    let mut pool: Arc<dyn Pool> = Arc::new(FakePool(state.clone()));
    let mut hook = Some(hook);
    for layer in 0..layers {
        let log = events.clone();
        pool = Arc::new(PolicyPool::from_shared(
            pool,
            PoolPolicy {
                telemetry_enabled: true,
                sql_capture_enabled: true,
                sql_hook: if layer == 0 { hook.take() } else { None },
                observer: Some(Arc::new(move |event| {
                    log.lock().unwrap().push(event.clone());
                })),
                retry: RetryPolicy::new(3, Delay::Fixed(Duration::ZERO), true).unwrap(),
                retry_runtime: Some(if layer + 1 == layers {
                    state.runtime()
                } else {
                    RetryRuntime::new(
                        Arc::new(Cancellation::new()),
                        || panic!("inner policy must not construct an independent timer"),
                        |_| panic!("inner policy must not retry a session operation"),
                        || 0,
                    )
                }),
                ..PoolPolicy::default()
            },
        ));
    }
    pool
}

#[test]
fn innermost_sql_hook_cannot_dispatch_after_cancelling_or_expiring_outer_budget() {
    for fetch in [false, true] {
        for interrupt in [Interrupt::Cancel, Interrupt::Deadline] {
            for layers in [2, 3, 5] {
                let state = Arc::new(State::default());
                state.acquire.open();
                state.statement.open();
                let hook_state = state.clone();
                let hook_calls = Arc::new(AtomicUsize::new(0));
                let calls = hook_calls.clone();
                let events = Arc::new(Mutex::new(Vec::new()));
                let pool = nested_pool(
                    &state,
                    layers,
                    Arc::new(move |_| {
                        calls.fetch_add(1, Ordering::SeqCst);
                        hook_state.apply(interrupt);
                        Some("safe replacement".to_owned())
                    }),
                    &events,
                );
                let mut work = operation(pool.as_ref(), fetch, options());
                assert_eq!(
                    work.as_mut().poll(&mut Context::from_waker(Waker::noop())),
                    Poll::Ready(Err(DbError::new(match interrupt {
                        Interrupt::Cancel => ErrorKind::Cancelled,
                        _ => ErrorKind::Timeout,
                    })))
                );
                state.assert_stopped(0);
                assert_eq!(hook_calls.load(Ordering::SeqCst), 1);
                assert!(events.lock().unwrap().is_empty(), "no SQL was dispatched");
            }
        }
    }
}

#[test]
fn nested_dispatched_results_keep_precedence_and_per_query_replacement_privacy() {
    for fetch in [false, true] {
        for interrupt in [Interrupt::Cancel, Interrupt::Deadline] {
            for kind in [
                None,
                Some(ErrorKind::WriteOutcomeUnknown),
                Some(ErrorKind::CommitOutcomeUnknown),
            ] {
                for layers in [2, 3, 5] {
                    let state = Arc::new(State::default());
                    state.acquire.open();
                    *state.error.lock().unwrap() = kind;
                    let events = Arc::new(Mutex::new(Vec::new()));
                    let default_calls = Arc::new(AtomicUsize::new(0));
                    let calls = default_calls.clone();
                    let default_state = state.clone();
                    let pool = nested_pool(
                        &state,
                        layers,
                        Arc::new(move |_| {
                            calls.fetch_add(1, Ordering::SeqCst);
                            default_state.cancellation.cancel();
                            Some("default must be replaced".to_owned())
                        }),
                        &events,
                    );
                    let hook_calls = Arc::new(AtomicUsize::new(0));
                    let calls = hook_calls.clone();
                    let mut work = operation(
                        pool.as_ref(),
                        fetch,
                        options().sql_hook(Arc::new(move |_| {
                            calls.fetch_add(1, Ordering::SeqCst);
                            Some("safe replacement".to_owned())
                        })),
                    );
                    let mut cx = Context::from_waker(Waker::noop());
                    assert!(work.as_mut().poll(&mut cx).is_pending());
                    assert_eq!(state.dispatches.load(Ordering::SeqCst), 1);
                    assert_eq!(hook_calls.load(Ordering::SeqCst), layers);
                    state.statement.open();
                    state.apply(interrupt);
                    assert_eq!(
                        work.as_mut().poll(&mut cx),
                        Poll::Ready(kind.map_or(Ok(()), |kind| Err(DbError::new(kind))))
                    );
                    state.assert_stopped(1);
                    assert_eq!(default_calls.load(Ordering::SeqCst), 0);
                    assert_eq!(hook_calls.load(Ordering::SeqCst), layers);
                    let events = events.lock().unwrap();
                    assert_eq!(events.len(), layers);
                    assert!(events.iter().all(|event| event.sql.as_deref()
                        == Some("safe replacement")
                        && event.outcome
                            == kind.map_or(TelemetryOutcome::Success, TelemetryOutcome::Error)));
                }
            }
        }
    }
}

#[test]
fn nested_telemetry_and_capture_off_suppress_pool_hooks_and_private_sql() {
    for fetch in [false, true] {
        for telemetry_off in [false, true] {
            for layers in [2, 3, 5] {
                let state = Arc::new(State::default());
                state.acquire.open();
                state.statement.open();
                let hook_calls = Arc::new(AtomicUsize::new(0));
                let calls = hook_calls.clone();
                let hook_state = state.clone();
                let events = Arc::new(Mutex::new(Vec::new()));
                let pool = nested_pool(
                    &state,
                    layers,
                    Arc::new(move |_| {
                        calls.fetch_add(1, Ordering::SeqCst);
                        hook_state.cancellation.cancel();
                        Some("hook must not run".to_owned())
                    }),
                    &events,
                );
                let mut work = operation(
                    pool.as_ref(),
                    fetch,
                    options()
                        .telemetry(if telemetry_off {
                            QuerySetting::Off
                        } else {
                            QuerySetting::Inherit
                        })
                        .sql_capture(if telemetry_off {
                            QuerySetting::Inherit
                        } else {
                            QuerySetting::Off
                        }),
                );
                assert_eq!(
                    work.as_mut().poll(&mut Context::from_waker(Waker::noop())),
                    Poll::Ready(Ok(()))
                );
                state.assert_stopped(1);
                assert_eq!(hook_calls.load(Ordering::SeqCst), 0);
                let events = events.lock().unwrap();
                assert_eq!(events.len(), if telemetry_off { 0 } else { layers });
                assert!(events.iter().all(|event| event.sql.is_none()));
            }
        }
    }
}
