package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// --- Public Types (Options & Payloads) ---

// Tx represents a database transaction interface that supports
// executing queries within a transaction context.
// This interface is compatible with pgx.Tx and allows for easier testing.
type Tx interface {
	// Exec executes a query that doesn't return rows (INSERT, UPDATE, DELETE, etc.)
	Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error)
	
	// Query executes a query that returns multiple rows
	Query(ctx context.Context, sql string, arguments ...interface{}) (pgx.Rows, error)
	
	// QueryRow executes a query that returns a single row
	QueryRow(ctx context.Context, sql string, arguments ...interface{}) pgx.Row
}

// JsonObject represents a JSON object.
type JsonObject = map[string]any

// RetryStrategy defines how a task should be retried.
type RetryStrategy struct {
	Kind        string  `json:"kind"` // "fixed", "exponential", "none"
	BaseSeconds float64 `json:"base_seconds,omitempty"`
	Factor      float64 `json:"factor,omitempty"`
	MaxSeconds  float64 `json:"max_seconds,omitempty"`
}

// CancellationPolicy defines task cancellation rules.
type CancellationPolicy struct {
	MaxDuration int64 `json:"max_duration,omitempty"` // in seconds
	MaxDelay    int64 `json:"max_delay,omitempty"`    // in seconds
}

// SpawnOptions configures a new task.
type SpawnOptions struct {
	MaxAttempts   int                `json:"max_attempts,omitempty"`
	RetryStrategy *RetryStrategy     `json:"retry_strategy,omitempty"`
	Headers       JsonObject         `json:"headers,omitempty"`
	Queue         string             `json:"-"` // Not part of the JSON, used for routing
	Cancellation  *CancellationPolicy `json:"cancellation,omitempty"`
}

// SpawnResult is returned after spawning a task.
type SpawnResult struct {
	TaskID  string
	RunID   string
	Attempt int
}

// ClaimedTask is the struct returned from the database when claiming work.
type ClaimedTask[P any, R any] struct {
	RunID          string          `json:"run_id"`
	TaskID         string          `json:"task_id"`
	TaskName       string          `json:"task_name"`
	Attempt        int             `json:"attempt"`
	Params         P               `json:"params"`
	RetryStrategy  json.RawMessage `json:"retry_strategy"`
	MaxAttempts    *int            `json:"max_attempts"`
	Headers        json.RawMessage `json:"headers"`
	WakeEvent      *string         `json:"wake_event"`
	EventPayload   json.RawMessage `json:"event_payload"`
}

// WorkerOptions configures the worker.
type WorkerOptions struct {
	WorkerID     string
	ClaimTimeout time.Duration
	BatchSize    int
	Concurrency  int
	PollInterval time.Duration
	OnError      func(error)
}

// TaskHandler is the generic function signature for a task.
// P is the type of the parameters.
// R is the type of the result.
type TaskHandler[P any, R any] func(ctx context.Context, params P, taskCtx *TaskContext[P, R]) (R, error)

type registeredTask[P any, R any] struct {
	Name                 string
	Queue                string
	DefaultMaxAttempts   int
	DefaultCancellation  *CancellationPolicy
	Handler              TaskHandler[P, R]
}


// --- TaskContext ---

// TaskContext is passed to every TaskHandler to interact with the workflow.
type TaskContext[P any, R any] struct {
	pool            *pgxpool.Pool
	queueName       string
	task            *ClaimedTask[P, R]
	claimTimeoutSec int
	checkpointCache *sync.Map // concurrent map
	stepNameCounter *sync.Map // concurrent map
}

// NewTaskContext creates a new task context and pre-loads checkpoints.
func NewTaskContext[P any, R any](ctx context.Context, pool *pgxpool.Pool, queueName string, task *ClaimedTask[P, R], claimTimeout time.Duration) (*TaskContext[P, R], error) {
	cache := &sync.Map{}
	query := `SELECT checkpoint_name, state
              FROM absurd.get_task_checkpoint_states($1, $2, $3)`

	ctx = context.WithoutCancel(ctx)
	rows, err := pool.Query(ctx, query, queueName, task.TaskID, task.RunID)
	if err != nil {
		return nil, fmt.Errorf("failed to get checkpoint states: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var state json.RawMessage
		if err := rows.Scan(&name, &state); err != nil {
			return nil, fmt.Errorf("failed to scan checkpoint: %w", err)
		}
		cache.Store(name, state)
	}
	
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating checkpoint states: %w", err)
	}

	return &TaskContext[P, R]{
		pool:            pool,
		queueName:       queueName,
		task:            task,
		claimTimeoutSec: int(claimTimeout.Seconds()),
		checkpointCache: cache,
		stepNameCounter: &sync.Map{},
	}, nil
}

func (c *TaskContext[P, R]) getCheckpointName(name string) string {
	val, _ := c.stepNameCounter.LoadOrStore(name, 0)
	count := val.(int) + 1
	c.stepNameCounter.Store(name, count)
	if count == 1 {
		return name
	}
	return fmt.Sprintf("%s#%d", name, count)
}

// lookupCheckpoint remains internal and returns raw JSON.
// The generic public methods (Step, AwaitEvent) will handle unmarshaling.
func (c *TaskContext[P, R]) lookupCheckpoint(ctx context.Context, checkpointName string) (json.RawMessage, bool, error) {
	if val, ok := c.checkpointCache.Load(checkpointName); ok {
		return val.(json.RawMessage), true, nil
	}

	query := `SELECT state FROM absurd.get_task_checkpoint_state($1, $2, $3)`
	var state json.RawMessage
	err := c.pool.QueryRow(ctx, query, c.queueName, c.task.TaskID, checkpointName).Scan(&state)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return nil, false, err
		}
		// pgx.ErrNoRows is fine, it just means no checkpoint
		return nil, false, nil
	}

	c.checkpointCache.Store(checkpointName, state)
	return state, true, nil
}

// persistCheckpoint remains internal and takes `any` to marshal.
func (c *TaskContext[P, R]) persistCheckpoint(ctx context.Context, tx Tx, checkpointName string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint state: %w", err)
	}

	query := `SELECT absurd.set_task_checkpoint_state($1, $2, $3, $4, $5, $6)`
	_, err = tx.Exec(ctx, query,
		c.queueName,
		c.task.TaskID,
		checkpointName,
		data,
		c.task.RunID,
		c.claimTimeoutSec,
	)
	if err != nil {
		return fmt.Errorf("failed to set checkpoint state: %w", err)
	}
	c.checkpointCache.Store(checkpointName, json.RawMessage(data))
	return nil
}

// Step executes a function idempotently, caching its result.
// It is generic over the return type TResult, allowing each step to have its own type.
// This is a standalone generic function that works with any TaskContext.
// The step function receives a context and database transaction that is automatically committed
// after successful execution and checkpoint persistence, or rolled back on error.
func Step[P any, R any, TResult any](taskCtx *TaskContext[P, R], ctx context.Context, name string, fn func(ctx context.Context, tx Tx) (TResult, error)) (TResult, error) {
	checkpointName := taskCtx.getCheckpointName(name)
	
	// zero-value for the return type TResult
	var result TResult

	state, ok, err := taskCtx.lookupCheckpoint(ctx, checkpointName)
	if err != nil {
		return result, err // return zero TResult and error
	}
	if ok {
		// We have a checkpoint, unmarshal it into our specific type TResult.
		if err := json.Unmarshal(state, &result); err != nil {
			return result, fmt.Errorf("failed to unmarshal checkpoint state: %w", err)
		}
		return result, nil
	}

	// No checkpoint, begin a transaction and run the function
	tx, err := taskCtx.pool.Begin(ctx)
	if err != nil {
		return result, fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Execute the step function with the context and transaction
	result, err = fn(ctx, tx)
	if err != nil {
		// Rollback transaction on error
		if rbErr := tx.Rollback(ctx); rbErr != nil {
			return result, fmt.Errorf("step failed and rollback also failed: %w (rollback error: %v)", err, rbErr)
		}
		return result, err
	}

	// Persist the result within the same transaction
	if err := taskCtx.persistCheckpoint(ctx, tx, checkpointName, result); err != nil {
		// Rollback transaction on checkpoint persistence error
		if rbErr := tx.Rollback(ctx); rbErr != nil {
			return result, fmt.Errorf("checkpoint persistence failed and rollback also failed: %w (rollback error: %v)", err, rbErr)
		}
		return result, err
	}

	// Commit the transaction
	if err := tx.Commit(ctx); err != nil {
		return result, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return result, nil
}

// SleepFor suspends the task for a given duration.
func (c *TaskContext[P, R]) SleepFor(ctx context.Context, stepName string, duration time.Duration) error {
	return c.SleepUntil(ctx, stepName, time.Now().Add(duration))
}

// SleepUntil suspends the task until a specific time.
func (c *TaskContext[P, R]) SleepUntil(ctx context.Context, stepName string, wakeAt time.Time) error {
	checkpointName := c.getCheckpointName(stepName)
	state, ok, err := c.lookupCheckpoint(ctx, checkpointName)
	if err != nil {
		return err
	}

	actualWakeAt := wakeAt
	if ok {
		// If checkpoint exists, parse its time to ensure we wake at the original time
		var timeStr string
		if err := json.Unmarshal(state, &timeStr); err == nil {
			if t, err := time.Parse(time.RFC3339Nano, timeStr); err == nil {
				actualWakeAt = t
			}
		}
	} else {
		// If no checkpoint, persist the wake-up time within a transaction
		tx, err := c.pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}
		if err := c.persistCheckpoint(ctx, tx, checkpointName, actualWakeAt.Format(time.RFC3339Nano)); err != nil {
			if rbErr := tx.Rollback(ctx); rbErr != nil {
				return fmt.Errorf("checkpoint persistence failed and rollback also failed: %w (rollback error: %v)", err, rbErr)
			}
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}
	}

	if time.Now().Before(actualWakeAt) {
		query := `SELECT absurd.schedule_run($1, $2, $3)`
		_, err := c.pool.Exec(ctx, query, c.queueName, c.task.RunID, actualWakeAt)
		if err != nil {
			return fmt.Errorf("failed to schedule run: %w", err)
		}
		return ErrSuspendTask
	}

	return nil
}

// AwaitEvent waits for an event to be emitted.
// It is generic over the expected payload type TPayload, allowing each event to have its own type.
// This is a standalone generic function that works with any TaskContext.
func AwaitEvent[P any, R any, TPayload any](taskCtx *TaskContext[P, R], ctx context.Context, eventName string, stepName string, timeout *int) (TPayload, error) {
	if stepName == "" {
		stepName = "$awaitEvent:" + eventName
	}
	checkpointName := taskCtx.getCheckpointName(stepName)
	
	var result TPayload // zero-value for TPayload

	state, ok, err := taskCtx.lookupCheckpoint(ctx, checkpointName)
	if err != nil {
		return result, err
	}
	if ok {
		if err := json.Unmarshal(state, &result); err != nil {
			return result, fmt.Errorf("failed to unmarshal checkpoint: %w", err)
		}
		return result, nil
	}

	// Handle timeout on wake-up: if we have a wake event that matches but no payload, it's a timeout
	if taskCtx.task.WakeEvent != nil && *taskCtx.task.WakeEvent == eventName && taskCtx.task.EventPayload == nil {
		return result, ErrTimeout
	}

	query := `SELECT should_suspend, payload
              FROM absurd.await_event($1, $2, $3, $4, $5, $6)`

	var shouldSuspend bool
	var payload json.RawMessage

	err = taskCtx.pool.QueryRow(ctx, query,
		taskCtx.queueName,
		taskCtx.task.TaskID,
		taskCtx.task.RunID,
		checkpointName,
		eventName,
		timeout,
	).Scan(&shouldSuspend, &payload)

	if err != nil {
		return result, fmt.Errorf("failed to await event: %w", err)
	}

	if !shouldSuspend {
		taskCtx.checkpointCache.Store(checkpointName, payload)
		if err := json.Unmarshal(payload, &result); err != nil {
			return result, fmt.Errorf("failed to unmarshal event payload: %w", err)
		}
		return result, nil
	}

	return result, ErrSuspendTask
}

// EmitEvent emits an event from within a task.
// It is generic over the payload type TPayload, allowing each event to have its own type.
// This is a standalone generic function that works with any TaskContext.
func EmitEvent[P any, R any, TPayload any](taskCtx *TaskContext[P, R], ctx context.Context, eventName string, payload TPayload) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal event payload: %w", err)
	}
	query := `SELECT absurd.emit_event($1, $2, $3)`
	_, err = taskCtx.pool.Exec(ctx, query, taskCtx.queueName, eventName, data)
	if err != nil {
		return fmt.Errorf("failed to emit event: %w", err)
	}
	return nil
}

// Complete marks the task run as completed.
// It takes `any` because the `R` type is only known by the handler.
func (c *TaskContext[P, R]) Complete(ctx context.Context, result any) error {
	data, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("failed to marshal result: %w", err)
	}
	query := `SELECT absurd.complete_run($1, $2, $3)`
	_, err = c.pool.Exec(ctx, query, c.queueName, c.task.RunID, data)
	return err
}

// Fail marks the task run as failed.
func (c *TaskContext[P, R]) Fail(ctx context.Context, taskErr error) error {
	errData, err := json.Marshal(map[string]string{
		"name":    "Error",
		"message": taskErr.Error(),
	})
	if err != nil {
		return fmt.Errorf("failed to marshal error data: %w", err)
	}
	log.Printf("[absurd] task execution failed: %v", taskErr)
	query := `SELECT absurd.fail_run($1, $2, $3, $4)`
	_, err = c.pool.Exec(ctx, query, c.queueName, c.task.RunID, errData, nil)
	return err
}

// --- Absurd Client ---

// Absurd is the main client for interacting with the workflow system.
type Absurd[P any, R any] struct {
	pool               *pgxpool.Pool
	ownedPool          bool
	defaultQueueName   string
	defaultMaxAttempts int
	registry           *sync.Map // map[string]registeredTask
	worker             *Worker[P, R]
}

// New creates a new Absurd client from a connection string.
func New[P any, R any](ctx context.Context, dsn string, defaultQueueName string, defaultMaxAttempts int) (*Absurd[P, R], error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &Absurd[P, R]{
		pool:               pool,
		ownedPool:          true,
		defaultQueueName:   defaultQueueName,
		defaultMaxAttempts: defaultMaxAttempts,
		registry:           &sync.Map{},
	}, nil
}

// NewWithPool creates a new Absurd client with an existing pgxpool.Pool.
func NewWithPool[P any, R any](pool *pgxpool.Pool, defaultQueueName string, defaultMaxAttempts int) *Absurd[P, R] {
	return &Absurd[P, R]{
		pool:               pool,
		ownedPool:          false,
		defaultQueueName:   defaultQueueName,
		defaultMaxAttempts: defaultMaxAttempts,
		registry:           &sync.Map{},
	}
}

// RegisterTask registers a strongly-typed handler for a specific task name.
// It is generic over the parameter type P and result type R.
func (a *Absurd[P, R]) RegisterTask(
	name string,
	handler TaskHandler[P, R],
	queue string,
	defaultMaxAttempts *int,
	defaultCancellation *CancellationPolicy,
) error {
	if name == "" {
		return ErrTaskNameRequired
	}
	if queue == "" {
		queue = a.defaultQueueName
	}
	if queue == "" {
		return &TaskQueueError{TaskName: name}
	}

	maxAttempts := a.defaultMaxAttempts
	if defaultMaxAttempts != nil {
		if *defaultMaxAttempts < 1 {
			return ErrMaxAttemptsInvalid
		}
		maxAttempts = *defaultMaxAttempts
	}

	
	a.registry.Store(name, registeredTask[P, R]{
		Name:                 name,
		Queue:                queue,
		DefaultMaxAttempts:   maxAttempts,
		DefaultCancellation:  defaultCancellation,
		Handler:              handler,
	})
	return nil
}

// CreateQueue creates the tables for a new queue.
func (a *Absurd[P, R]) CreateQueue(ctx context.Context, queueName string) error {
	if queueName == "" {
		queueName = a.defaultQueueName
	}
	_, err := a.pool.Exec(ctx, `SELECT absurd.create_queue($1)`, queueName)
	return err
}

// DropQueue drops all tables for a queue.
func (a *Absurd[P, R]) DropQueue(ctx context.Context, queueName string) error {
	if queueName == "" {
		queueName = a.defaultQueueName
	}
	_, err := a.pool.Exec(ctx, `SELECT absurd.drop_queue($1)`, queueName)
	return err
}

// ListQueues lists all created queues.
func (a *Absurd[P, R]) ListQueues(ctx context.Context) ([]string, error) {
	rows, err := a.pool.Query(ctx, `SELECT * FROM absurd.list_queues()`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var queues []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		queues = append(queues, name)
	}
	
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating queues: %w", err)
	}
	
	return queues, nil
}

// Spawn enqueues a new task with strongly-typed parameters.
// It is generic over the parameter type P.
func (a *Absurd[P, R]) Spawn(
	ctx context.Context,
	taskName string,
	params P, // params are now strongly-typed
	opts SpawnOptions,
) (*SpawnResult, error) {
	var queue string
	var reg registeredTask[P, R]
	val, ok := a.registry.Load(taskName)
	if ok {
		reg = val.(registeredTask[P, R])
		queue = reg.Queue
		if opts.Queue != "" && opts.Queue != reg.Queue {
			return nil, &TaskQueueError{
				TaskName:      taskName,
				ExpectedQueue: reg.Queue,
				ActualQueue:   opts.Queue,
			}
		}
	} else if opts.Queue == "" {
		return nil, &TaskNotRegisteredError{TaskName: taskName}
	} else {
		queue = opts.Queue
	}

	if opts.MaxAttempts == 0 {
		if ok {
			opts.MaxAttempts = reg.DefaultMaxAttempts
		} else {
			opts.MaxAttempts = a.defaultMaxAttempts
		}
	}
	if opts.Cancellation == nil && ok {
		opts.Cancellation = reg.DefaultCancellation
	}

	// Marshal the strongly-typed params
	paramsData, err := json.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal params: %w", err)
	}

	optsData, err := json.Marshal(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal options: %w", err)
	}

	query := `SELECT task_id, run_id, attempt
              FROM absurd.spawn_task($1, $2, $3, $4)`

	var res SpawnResult
	err = a.pool.QueryRow(ctx, query, queue, taskName, paramsData, optsData).Scan(&res.TaskID, &res.RunID, &res.Attempt)
	if err != nil {
		return nil, fmt.Errorf("failed to spawn task: %w", err)
	}
	return &res, nil
}

// ClientEmitEvent emits an event from outside a task (from the Absurd client).
// It is generic over the payload type TPayload, allowing different event types.
// This is a standalone generic function that works with any Absurd client.
func ClientEmitEvent[P any, R any, TPayload any](client *Absurd[P, R], ctx context.Context, eventName string, payload TPayload, queueName string) error {
	if eventName == "" {
		return ErrEventNameRequired
	}
	if queueName == "" {
		queueName = client.defaultQueueName
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}
	_, err = client.pool.Exec(ctx, `SELECT absurd.emit_event($1, $2, $3)`, queueName, eventName, data)
	return err
}

func (a *Absurd[P, R]) claimTasks(ctx context.Context, queueName string, workerID string, claimTimeout time.Duration, count int) ([]*ClaimedTask[P, R], error) {
	query := `SELECT run_id, task_id, attempt, task_name, params, retry_strategy, max_attempts,
                     headers, wake_event, event_payload
              FROM absurd.claim_task($1, $2, $3, $4)`

	rows, err := a.pool.Query(ctx, query, queueName, workerID, int(claimTimeout.Seconds()), count)
	if err != nil {
		return nil, fmt.Errorf("failed to claim tasks: %w", err)
	}
	defer rows.Close()

	var tasks []*ClaimedTask[P, R]
	for rows.Next() {
		var t ClaimedTask[P, R]
		err := rows.Scan(
			&t.RunID, &t.TaskID, &t.Attempt, &t.TaskName, &t.Params,
			&t.RetryStrategy, &t.MaxAttempts, &t.Headers,
			&t.WakeEvent, &t.EventPayload,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan claimed task: %w", err)
		}
		tasks = append(tasks, &t)
	}
	
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating claimed tasks: %w", err)
	}
	
	return tasks, nil
}

func (a *Absurd[P, R]) executeTask(ctx context.Context, task *ClaimedTask[P, R], claimTimeout time.Duration, queueName string) error {
	val, ok := a.registry.Load(task.TaskName)
	if !ok {
		return &TaskNotFoundError{TaskName: task.TaskName}
	}
	reg := val.(registeredTask[P, R])

	if reg.Queue != queueName {
		return &QueueMismatchError{
			TaskName:      task.TaskName,
			ExpectedQueue: reg.Queue,
			ActualQueue:   queueName,
		}
	}

	taskCtx, err := NewTaskContext(ctx, a.pool, reg.Queue, task, claimTimeout)
	if err != nil {
		return fmt.Errorf("failed to create task context: %w", err)
	}

	handlerCtx, cancel := context.WithTimeout(ctx, claimTimeout)
	defer cancel()

	// Call the type-erased wrapper.
	// This wrapper will handle unmarshaling to the correct P type
	// and calling the user's generic handler.
	result, err := reg.Handler(handlerCtx, task.Params, taskCtx)
	if err != nil {
		if errors.Is(err, ErrSuspendTask) {
			// This is not a failure, just a suspension.
			return nil
		}
		// Task failed
		return taskCtx.Fail(ctx, err)
	}

	// Task completed. result is 'any'
	return taskCtx.Complete(ctx, result)
}

// --- Worker ---

// Worker manages the polling and execution of tasks.
type Worker[P any, R any] struct {
	absurd  *Absurd[P, R]
	options WorkerOptions
	queue   string
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// StartWorker begins polling for and executing tasks.
func (a *Absurd[P, R]) StartWorker(opts WorkerOptions, queueName string) (*Worker[P, R], error) {
	if queueName == "" {
		queueName = a.defaultQueueName
	}

	// Set defaults
	if opts.WorkerID == "" {
		hostname, _ := os.Hostname()
		opts.WorkerID = fmt.Sprintf("%s:%d", hostname, os.Getpid())
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = 1
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = opts.Concurrency
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = 250 * time.Millisecond
	}
	if opts.ClaimTimeout <= 0 {
		opts.ClaimTimeout = 120 * time.Second
	}
	if opts.OnError == nil {
		opts.OnError = func(err error) { log.Printf("Worker error: %v", err) }
	}

	ctx, cancel := context.WithCancel(context.Background())
	worker := &Worker[P, R]{
		absurd:  a,
		options: opts,
		queue:   queueName,
		cancel:  cancel,
	}

	a.worker = worker
	worker.wg.Add(1)
	go worker.run(ctx)

	return worker, nil
}

// Close gracefully shuts down the worker.
func (w *Worker[P, R]) Close() {
	w.cancel()
	w.wg.Wait()
}

// run is the main poller loop for the worker.
func (w *Worker[P, R]) run(ctx context.Context) {
	defer w.wg.Done()
	sem := make(chan struct{}, w.options.Concurrency)
	
	// Set up signal handler to gracefully shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	
	go func() {
		defer signal.Stop(sigChan)
		select {
		case <-sigChan:
			log.Println("Shutdown signal received, cancelling worker context...")
			w.cancel()
		case <-ctx.Done():
			// Context already cancelled, nothing to do
		}
	}()

	for {
		select {
		case <-ctx.Done():
			log.Println("Worker context cancelled, shutting down poller.")
			// Wait for all in-flight tasks to complete by draining the semaphore
			for i := 0; i < w.options.Concurrency; i++ {
				sem <- struct{}{}
			}
			return
		default:
		}

		tasks, err := w.absurd.claimTasks(
			ctx,
			w.queue,
			w.options.WorkerID,
			w.options.ClaimTimeout,
			w.options.BatchSize,
		)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				w.options.OnError(fmt.Errorf("failed to claim tasks: %w", err))
			}
			time.Sleep(w.options.PollInterval)
			continue
		}

		if len(tasks) == 0 {
			time.Sleep(w.options.PollInterval)
			continue
		}

		for _, task := range tasks {
			sem <- struct{}{}
			w.wg.Add(1)
			go func(t *ClaimedTask[P, R]) {
				defer w.wg.Done()
				defer func() { <-sem }()
				if ctx.Err() != nil {
					return
				}
				if err := w.absurd.executeTask(ctx, t, w.options.ClaimTimeout, w.queue); err != nil {
					w.options.OnError(fmt.Errorf("failed to execute task %s on queue %s: %w", t.TaskID, w.queue, err))
				}
			}(task)
			
		}
	}
}

// Close gracefully shuts down the worker and the connection pool (if owned).
func (a *Absurd[P, R]) Close() {
	if a.worker != nil {
		a.worker.Close()
	}
	if a.ownedPool {
		a.pool.Close()
	}
}