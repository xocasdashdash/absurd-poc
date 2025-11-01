package client

import (
	"context"
	"encoding/json"
	"fmt"
)

// TypedTaskHandler is a strongly-typed task handler that works with specific parameter and result types.
type TypedTaskHandler[P any, R any] func(ctx context.Context, params P, taskCtx *TaskContext[any, any]) (R, error)

// RegisterTypedTask registers a strongly-typed task handler with automatic serialization.
// This allows each task to have different parameter and result types while maintaining type safety.
//
// Example:
//
//	client := absurd.New[any, any](...)
//	absurd.RegisterTypedTask(client, "send-email", func(ctx context.Context, params SendEmailParams, taskCtx *TaskContext[any, any]) (SendEmailResult, error) {
//	    // Your typed implementation
//	    return SendEmailResult{Sent: true}, nil
//	}, "emails", nil, nil)
func RegisterTypedTask[P any, R any](
	client *Absurd[any, any],
	name string,
	handler TypedTaskHandler[P, R],
	queue string,
	defaultMaxAttempts *int,
	defaultCancellation *CancellationPolicy,
) error {
	// Wrap the typed handler in an untyped one that handles serialization
	wrappedHandler := func(ctx context.Context, params any, taskCtx *TaskContext[any, any]) (any, error) {
		// Deserialize params from map[string]any to P
		var typedParams P
		if params != nil {
			// Re-marshal and unmarshal to convert map[string]any to P
			data, err := json.Marshal(params)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal params: %w", err)
			}
			if err := json.Unmarshal(data, &typedParams); err != nil {
				return nil, fmt.Errorf("failed to unmarshal params to %T: %w", typedParams, err)
			}
		}

		// Call the typed handler
		result, err := handler(ctx, typedParams, taskCtx)
		if err != nil {
			return nil, err
		}

		// Return result (will be serialized by the client)
		return result, nil
	}

	return client.RegisterTask(name, wrappedHandler, queue, defaultMaxAttempts, defaultCancellation)
}

// SpawnTypedTask spawns a task with strongly-typed parameters.
//
// Example:
//
//	result, err := absurd.SpawnTypedTask(client, ctx, "send-email", SendEmailParams{
//	    To: "user@example.com",
//	    Subject: "Hello",
//	}, absurd.SpawnOptions{})
func SpawnTypedTask[P any](
	client *Absurd[any, any],
	ctx context.Context,
	taskName string,
	params P,
	opts SpawnOptions,
) (*SpawnResult, error) {
	// The client will handle JSON serialization automatically
	return client.Spawn(ctx, taskName, params, opts)
}

// TypedStep is a strongly-typed checkpoint step.
//
// Example:
//
//	users, err := absurd.TypedStep(taskCtx, ctx, "fetch-users", func() ([]User, error) {
//	    return fetchUsersFromDB()
//	})
func TypedStep[TResult any](
	taskCtx *TaskContext[any, any],
	ctx context.Context,
	name string,
	fn func() (TResult, error),
) (TResult, error) {
	return Step[any, any, TResult](taskCtx, ctx, name, fn)
}

// TypedAwaitEvent waits for an event with a strongly-typed payload.
//
// Example:
//
//	payment, err := absurd.TypedAwaitEvent[PaymentCompleted](taskCtx, ctx, "payment.completed", "wait-for-payment", nil)
func TypedAwaitEvent[TPayload any](
	taskCtx *TaskContext[any, any],
	ctx context.Context,
	eventName string,
	stepName string,
	timeout *int,
) (TPayload, error) {
	return AwaitEvent[any, any, TPayload](taskCtx, ctx, eventName, stepName, timeout)
}

// TypedEmitEvent emits an event with a strongly-typed payload.
//
// Example:
//
//	err := absurd.TypedEmitEvent(taskCtx, ctx, "order.created", OrderCreated{OrderID: "123"})
func TypedEmitEvent[TPayload any](
	taskCtx *TaskContext[any, any],
	ctx context.Context,
	eventName string,
	payload TPayload,
) error {
	return EmitEvent[any, any, TPayload](taskCtx, ctx, eventName, payload)
}
