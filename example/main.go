package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/xocasdashdash/absurd-poc/absurd/client"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// WebhookEvent represents the event data
type WebhookEvent struct {
	OrganizationID string                 `json:"organization_id"`
	EventName      string                 `json:"event_name"`
	Payload        map[string]interface{} `json:"payload"`
	Timestamp      string                 `json:"timestamp"`
}

// RouteNotificationParams are the parameters for the route-notification task
type RouteNotificationParams struct {
	Event WebhookEvent `json:"event"`
}

// SendWebhookParams are the parameters for the send-webhook task
type SendWebhookParams struct {
	DestinationURL string       `json:"destination_url"`
	Event          WebhookEvent `json:"event"`
	DestinationID  int          `json:"destination_id"`
}

// EmptyResult is used for tasks that don't return a value
type EmptyResult struct{}

// Destination represents a webhook destination from the database
type Destination struct {
	ID             int
	OrganizationID string
	EventName      string
	WebhookURL     string
}

func main() {
	mode := flag.String("mode", "worker", "Run mode: 'api' or 'worker'")
	flag.Parse()

	ctx := context.Background()

	// Get database URL from environment
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Fatal("DATABASE_URL environment variable is required")
	}

	switch *mode {
	case "api":
		runAPI(ctx, dbURL)
	case "worker":
		runWorker(ctx, dbURL)
	default:
		log.Fatalf("Invalid mode: %s. Must be 'api' or 'worker'", *mode)
	}
}

func runAPI(ctx context.Context, dbURL string) {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// Create Absurd client using any/any for flexibility
	absurdClient, err := client.New[any, any](
		ctx,
		dbURL,
		"notifications",
		3, // default max attempts
	)
	if err != nil {
		log.Fatalf("Failed to create Absurd client: %v", err)
	}
	defer absurdClient.Close()

	// Register tasks (without handlers) so the client knows which queue they belong to
	// This allows spawning without explicitly specifying the queue in SpawnOptions
	err = client.RegisterTypedTask[RouteNotificationParams, EmptyResult](
		absurdClient,
		"route-notification",
		nil,
		"notifications",
		nil,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to register route-notification task: %v", err)
	}

	log.Println("Starting API server...")
	log.Printf("Database: %s", dbURL)
	log.Printf("Port: %s", port)

	// Create HTTP server
	mux := http.NewServeMux()

	// Health check endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
	})

	// Webhook endpoint
	mux.HandleFunc("/webhook", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var event WebhookEvent
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			http.Error(w, fmt.Sprintf("Invalid JSON: %v", err), http.StatusBadRequest)
			return
		}

		// Validate required fields
		if event.OrganizationID == "" {
			http.Error(w, "organization_id is required", http.StatusBadRequest)
			return
		}
		if event.EventName == "" {
			http.Error(w, "event_name is required", http.StatusBadRequest)
			return
		}

		// Add timestamp if not provided
		if event.Timestamp == "" {
			event.Timestamp = time.Now().UTC().Format(time.RFC3339)
		}

		// Spawn route-notification task with type safety
		params := RouteNotificationParams{Event: event}
		result, err := client.SpawnTypedTask(
			absurdClient,
			r.Context(),
			"route-notification",
			params,
			client.SpawnOptions{
				MaxAttempts: 3,
				RetryStrategy: &client.RetryStrategy{
					Kind:        "exponential",
					BaseSeconds: 1,
					Factor:      2,
					MaxSeconds:  30,
				},
			},
		)
		if err != nil {
			log.Printf("Error spawning task: %v", err)
			http.Error(w, "Failed to enqueue notification", http.StatusInternalServerError)
			return
		}

		log.Printf("Webhook received - TaskID: %s, Event: %s, Org: %s",
			result.TaskID, event.EventName, event.OrganizationID)

		// Return success response
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":  "accepted",
			"task_id": result.TaskID,
			"run_id":  result.RunID,
			"message": "Notification queued for processing",
		})
	})

	server := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	// Start server in a goroutine
	go func() {
		log.Printf("API server listening on :%s", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down API server...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("Server shutdown error: %v", err)
	}

	log.Println("API server stopped")
}

func runWorker(ctx context.Context, dbURL string) {
	workerID := os.Getenv("HOSTNAME")
	if workerID == "" {
		workerID = "worker-1"
	}

	log.Printf("Starting worker: %s", workerID)
	log.Printf("Database: %s", dbURL)

	// Open database connection for querying destinations
	db, err := sql.Open("pgx", dbURL)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Create Absurd client using 'any' type to handle multiple task types
	absurdClient, err := client.New[any, any](
		ctx,
		dbURL,
		"notifications",
		3,
	)
	if err != nil {
		log.Fatalf("Failed to create Absurd client: %v", err)
	}
	defer absurdClient.Close()

	// Register route-notification task handler with type safety
	err = client.RegisterTypedTask[RouteNotificationParams, EmptyResult](
		absurdClient,
		"route-notification",
		func(ctx context.Context, params RouteNotificationParams, taskCtx *client.TaskContext[any, any]) (EmptyResult, error) {
			log.Printf("[route-notification] Processing event: %s for org: %s",
				params.Event.EventName, params.Event.OrganizationID)

			// Step 1: Query destinations from database
			destinations, err := client.TypedStep(taskCtx, ctx, "query-destinations", func(ctx context.Context, tx client.Tx) ([]Destination, error) {
				return queryDestinations(ctx, tx, params.Event.OrganizationID, params.Event.EventName)
			})
			if err != nil {
				return EmptyResult{}, fmt.Errorf("failed to query destinations: %w", err)
			}

			log.Printf("[route-notification] Found %d destinations", len(destinations))

			if len(destinations) == 0 {
				log.Printf("[route-notification] No destinations found for event: %s, org: %s",
					params.Event.EventName, params.Event.OrganizationID)
				return EmptyResult{}, nil
			}

			// Step 2: Spawn send-webhook tasks for each destination
			_, err = client.TypedStep(taskCtx, ctx, "spawn-webhook-tasks", func(ctx context.Context, tx client.Tx) (int, error) {
				count := 0
				for _, dest := range destinations {
					webhookParams := SendWebhookParams{
						DestinationURL: dest.WebhookURL,
						Event:          params.Event,
						DestinationID:  dest.ID,
					}

					_, err := client.SpawnTypedTask(
						absurdClient,
						ctx,
						"send-webhook",
						webhookParams,
						client.SpawnOptions{
							MaxAttempts: 5,
							RetryStrategy: &client.RetryStrategy{
								Kind:        "exponential",
								BaseSeconds: 2,
								Factor:      2,
								MaxSeconds:  60,
							},
						},
					)
					if err != nil {
						log.Printf("[route-notification] Failed to spawn send-webhook task: %v", err)
						continue
					}
					count++
				}

				log.Printf("[route-notification] Spawned %d send-webhook tasks", count)
				return count, nil
			})

			if err != nil {
				return EmptyResult{}, fmt.Errorf("failed to spawn webhook tasks: %w", err)
			}

			return EmptyResult{}, nil
		},
		"",
		nil,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to register route-notification task: %v", err)
	}

	// Register send-webhook task handler with type safety
	err = client.RegisterTypedTask[SendWebhookParams, EmptyResult](
		absurdClient,
		"send-webhook",
		func(ctx context.Context, params SendWebhookParams, taskCtx *client.TaskContext[any, any]) (EmptyResult, error) {
			log.Printf("[send-webhook] Sending to: %s (dest_id: %d)",
				params.DestinationURL, params.DestinationID)

			// Step 1: Send HTTP POST request
			_, err := client.TypedStep(taskCtx, ctx, "http-post", func(ctx context.Context, tx client.Tx) (string, error) {
				return sendWebhookHTTP(params.DestinationURL, params.Event)
			})
			if err != nil {
				return EmptyResult{}, fmt.Errorf("failed to send webhook: %w", err)
			}

			log.Printf("[send-webhook] Successfully sent to: %s", params.DestinationURL)
			return EmptyResult{}, nil
		},
		"",
		nil,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to register send-webhook task: %v", err)
	}

	// Start a single worker that handles both task types
	log.Println("Starting worker...")
	worker, err := absurdClient.StartWorker(
		client.WorkerOptions{
			WorkerID:     workerID,
			Concurrency:  10,
			BatchSize:    20,
			PollInterval: 250 * time.Millisecond,
			ClaimTimeout: 30 * time.Second,
		},
		"notifications",
	)
	if err != nil {
		log.Fatalf("Failed to start worker: %v", err)
	}
	defer worker.Close()

	log.Println("Worker started successfully. Processing tasks from queue: notifications")

	// Block forever - worker handles shutdown signals
	select {}
}

// queryDestinations fetches webhook destinations from the database
func queryDestinations(ctx context.Context, tx client.Tx, organizationID, eventName string) ([]Destination, error) {
	query := `
		SELECT id, organization_id, event_name, webhook_url
		FROM notification_destinations
		WHERE organization_id = $1 AND event_name = $2 AND enabled = true
	`

	rows, err := tx.Query(ctx, query, organizationID, eventName)
	if err != nil {
		return nil, fmt.Errorf("database query failed: %w", err)
	}
	defer rows.Close()

	var destinations []Destination
	for rows.Next() {
		var dest Destination
		if err := rows.Scan(&dest.ID, &dest.OrganizationID, &dest.EventName, &dest.WebhookURL); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		destinations = append(destinations, dest)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}

	return destinations, nil
}

// sendWebhookHTTP sends the webhook via HTTP POST
func sendWebhookHTTP(url string, event WebhookEvent) (string, error) {
	payload, err := json.Marshal(event)
	if err != nil {
		return "", fmt.Errorf("failed to marshal payload: %w", err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewReader(payload))
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "Absurd-Notification-Router/1.0")

	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("webhook endpoint returned error %d: %s", resp.StatusCode, string(body))
	}

	return fmt.Sprintf("Status: %d", resp.StatusCode), nil
}
