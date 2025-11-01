package client

import (
	"errors"
	"fmt"
)

// --- Errors ---

// Sentinel errors for common cases
var (
	// ErrSuspendTask is a sentinel error used to signal that a task should
	// be suspended (e.g., due to sleep or awaitEvent).
	ErrSuspendTask = errors.New("task suspended")

	// ErrTimeout is returned when awaiting an event times out.
	ErrTimeout = errors.New("timed out waiting for event")

	// ErrTaskNameRequired is returned when attempting to register a task without a name.
	ErrTaskNameRequired = errors.New("task registration requires a name")

	// ErrMaxAttemptsInvalid is returned when defaultMaxAttempts is less than 1.
	ErrMaxAttemptsInvalid = errors.New("defaultMaxAttempts must be at least 1")

	// ErrEventNameRequired is returned when attempting to emit an event without a name.
	ErrEventNameRequired = errors.New("eventName must be a non-empty string")
)

// TaskQueueError represents errors related to task queue configuration.
type TaskQueueError struct {
	TaskName      string
	ExpectedQueue string
	ActualQueue   string
}

func (e *TaskQueueError) Error() string {
	if e.ActualQueue == "" {
		return fmt.Sprintf("task %q must specify a queue or use a client with a default queue", e.TaskName)
	}
	return fmt.Sprintf("task %q is registered for queue %q but spawn requested queue %q", 
		e.TaskName, e.ExpectedQueue, e.ActualQueue)
}

// TaskNotRegisteredError is returned when attempting to spawn an unregistered task.
type TaskNotRegisteredError struct {
	TaskName string
}

func (e *TaskNotRegisteredError) Error() string {
	return fmt.Sprintf("task %q is not registered. Provide options.Queue when spawning unregistered tasks", e.TaskName)
}

// TaskNotFoundError is returned when a claimed task is not in the registry.
type TaskNotFoundError struct {
	TaskName string
}

func (e *TaskNotFoundError) Error() string {
	return fmt.Sprintf("unknown task: %s", e.TaskName)
}

// QueueMismatchError is returned when a task's queue doesn't match expectations.
type QueueMismatchError struct {
	TaskName      string
	ExpectedQueue string
	ActualQueue   string
}

func (e *QueueMismatchError) Error() string {
	return fmt.Sprintf("misconfigured task (queue mismatch: expected %s, got %s)", e.ExpectedQueue, e.ActualQueue)
}