# Type-Safe Pattern for Absurd Tasks

This document demonstrates the recommended pattern for working with multiple task types in Absurd while maintaining type safety.

## Problem

The basic `Absurd[P, R]` client is generic over a single parameter type `P` and result type `R`. This means:
- All tasks registered with that client must use the same types
- To support multiple task types, you'd need to use `any` and manual type assertions
- This loses type safety and is error-prone

## Solution: Type-Safe Wrapper Functions

Use `Absurd[any, any]` as the base client, but register tasks using type-safe wrapper functions that handle serialization/deserialization automatically.

## Example Usage

### 1. Define Your Task Types

```go
// Task 1: Send Email
type SendEmailParams struct {
    To      string `json:"to"`
    Subject string `json:"subject"`
    Body    string `json:"body"`
}

type SendEmailResult struct {
    MessageID string `json:"message_id"`
    SentAt    string `json:"sent_at"`
}

// Task 2: Process Payment
type ProcessPaymentParams struct {
    OrderID string  `json:"order_id"`
    Amount  float64 `json:"amount"`
}

type ProcessPaymentResult struct {
    TransactionID string `json:"transaction_id"`
    Status        string `json:"status"`
}
```

### 2. Create a Single Client

```go
import (
    "context"
    "github.com/xocasdashdash/absurd-poc/absurd/client"
)

// Use any/any types for the base client
absurdClient, err := client.New[any, any](ctx, dbURL, "default", 3)
if err != nil {
    log.Fatal(err)
}
defer absurdClient.Close()
```

### 3. Register Tasks with Type Safety

```go
// Register send-email task - fully typed!
err = client.RegisterTypedTask(
    absurdClient,
    "send-email",
    func(ctx context.Context, params SendEmailParams, taskCtx *client.TaskContext[any, any]) (SendEmailResult, error) {
        // params is SendEmailParams - no type assertions needed!
        log.Printf("Sending email to %s", params.To)
        
        // Use typed steps with transaction support
        messageID, err := client.TypedStep(taskCtx, ctx, "send", func(ctx context.Context, tx client.Tx) (string, error) {
            // Send the email (can use tx for database queries)
            return "msg-123", nil
        })
        if err != nil {
            return SendEmailResult{}, err
        }
        
        return SendEmailResult{
            MessageID: messageID,
            SentAt:    time.Now().Format(time.RFC3339),
        }, nil
    },
    "default",
    nil,
    nil,
)

// Register process-payment task - different types!
err = client.RegisterTypedTask(
    absurdClient,
    "process-payment",
    func(ctx context.Context, params ProcessPaymentParams, taskCtx *client.TaskContext[any, any]) (ProcessPaymentResult, error) {
        // params is ProcessPaymentParams
        log.Printf("Processing payment for order %s: $%.2f", params.OrderID, params.Amount)
        
        // Use typed steps with different return types and transaction support
        txnID, err := client.TypedStep(taskCtx, ctx, "charge-card", func(ctx context.Context, tx client.Tx) (string, error) {
            // Charge the card (can use tx for database queries)
            return "txn-456", nil
        })
        if err != nil {
            return ProcessPaymentResult{}, err
        }
        
        return ProcessPaymentResult{
            TransactionID: txnID,
            Status:        "completed",
        }, nil
    },
    "default",
    nil,
    nil,
)
```

### 4. Spawn Tasks with Type Safety

```go
// Spawn send-email - compile-time type checking!
result1, err := client.SpawnTypedTask(
    absurdClient,
    ctx,
    "send-email",
    SendEmailParams{
        To:      "user@example.com",
        Subject: "Welcome!",
        Body:    "Thanks for signing up",
    },
    client.SpawnOptions{},
)

// Spawn process-payment - different types!
result2, err := client.SpawnTypedTask(
    absurdClient,
    ctx,
    "process-payment",
    ProcessPaymentParams{
        OrderID: "ord-123",
        Amount:  99.99,
    },
    client.SpawnOptions{
        MaxAttempts: 5,
    },
)
```

### 5. Start a Single Worker

```go
// One worker handles both task types!
worker, err := absurdClient.StartWorker(
    client.WorkerOptions{
        WorkerID:     "worker-1",
        Concurrency:  10,
        PollInterval: 500 * time.Millisecond,
    },
    "default",
)
```

## Benefits

✅ **Type Safety**: Full compile-time type checking for parameters and results  
✅ **Single Client**: One client handles all task types  
✅ **Single Worker**: One worker processes all task types  
✅ **No Type Assertions**: Automatic serialization/deserialization  
✅ **Typed Steps**: Each step can have its own return type  
✅ **Clean Code**: No manual JSON marshaling  

## API Reference

### `RegisterTypedTask[P, R](client, name, handler, queue, maxAttempts, cancellation)`
Registers a task with strong typing for parameters and results.

### `SpawnTypedTask[P](client, ctx, name, params, opts)`
Spawns a task with strongly-typed parameters.

### `TypedStep[TResult](taskCtx, ctx, name, fn)`
Creates a checkpointed step with a strongly-typed return value.

### `TypedAwaitEvent[TPayload](taskCtx, ctx, eventName, stepName, timeout)`
Waits for an event with a strongly-typed payload.

### `TypedEmitEvent[TPayload](taskCtx, ctx, eventName, payload)`
Emits an event with a strongly-typed payload.

## Migration Guide

### Before (using any with manual assertions):

```go
client, _ := client.New[any, any](ctx, dbURL, "default", 3)

client.RegisterTask("send-email", 
    func(ctx context.Context, params any, taskCtx *client.TaskContext[any, any]) (any, error) {
        // Manual type assertion - error prone!
        emailParams, ok := params.(map[string]any)
        if !ok {
            return nil, fmt.Errorf("invalid params type")
        }
        to, _ := emailParams["to"].(string)
        
        // ... rest of implementation
        return map[string]any{"message_id": "123"}, nil
    },
    "default", nil, nil,
)
```

### After (using typed helpers):

```go
client, _ := client.New[any, any](ctx, dbURL, "default", 3)

client.RegisterTypedTask(client, "send-email",
    func(ctx context.Context, params SendEmailParams, taskCtx *client.TaskContext[any, any]) (SendEmailResult, error) {
        // Fully typed - compiler catches errors!
        log.Printf("Sending to: %s", params.To)
        
        return SendEmailResult{MessageID: "123"}, nil
    },
    "default", nil, nil,
)
```

## Complete Example

See `/example/main.go` for a complete working example using this pattern.
