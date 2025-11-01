.PHONY: help build docker-build up down logs create-queue setup test-webhook list-tasks dump-task clean db-shell db-connect db-reset db-init db-status

BINARY_NAME=absurd-example

help:
	@echo "Usage:"
	@echo "  make build        - Build the Go binary locally"
	@echo "  make docker-build - Build the Docker image with the binary"
	@echo "  make up           - Start all services (PostgreSQL, API, Worker)"
	@echo "  make down         - Stop all services"
	@echo "  make logs         - View logs for all services"
	@echo "  make setup        - Full setup: build + docker-build + up + create-queue"
	@echo "  make create-queue - Create the 'notifications' queue"
	@echo "  make test-webhook - Send test webhooks"
	@echo "  make list-tasks   - List recent tasks"
	@echo "  make dump-task    - Dump task details (usage: make dump-task TASK_ID=xxx)"
	@echo "  make db-shell     - Open psql shell in the database container"
	@echo "  make db-connect   - Connection string for external psql/tools"
	@echo "  make db-status    - Check database schema and queue status"
	@echo "  make db-init      - Initialize/reinitialize the Absurd schema"
	@echo "  make db-reset     - Reset database (WARNING: deletes all data)"
	@echo "  make clean        - Remove all containers, volumes, and binary"

build:
	@echo "Building Go binary..."
	CGO_ENABLED=0 GOOS=linux go build -o $(BINARY_NAME) .
	@echo "✓ Binary built: $(BINARY_NAME)"

docker-build: build
	@echo "Building Docker image..."
	docker build -t absurd-example:latest .
	@echo "✓ Docker image built: absurd-example:latest"

up:
	@echo "Starting services..."
	docker-compose up -d postgres
	@echo "Waiting for PostgreSQL to initialize..."
	@sleep 8
	@echo "Initializing database..."
	@$(MAKE) --no-print-directory db-init || true
	@echo "Starting API, Worker, and Webhook.site..."
	docker-compose up -d api worker webhook-site
	@echo ""
	@echo "==================================="
	@echo "Services started!"
	@echo "API:          http://localhost:18080"
	@echo "Webhook.site: http://localhost:19090"
	@echo "==================================="

down:
	docker-compose down

logs:
	docker-compose logs -f

create-queue:
	@echo "Creating notifications queue..."
	@docker-compose exec -T postgres psql -U absurd -d absurd -c "SELECT absurd.create_queue('notifications');" 2>/dev/null || true
	@echo "✓ Queue ready"

setup: docker-build up
	@echo ""
	@echo "==================================="
	@echo "Setup complete!"
	@echo ""
	@echo "URLs:"
	@echo "  API:          http://localhost:18080"
	@echo "  Webhook.site: http://localhost:19090"
	@echo ""
	@echo "Try it:"
	@echo "  make test-webhook"
	@echo "  make list-tasks"
	@echo "  open http://localhost:19090"
	@echo "==================================="

test-webhook:
	@echo "Sending test webhook (user.created for org-123)..."
	@curl -X POST http://localhost:18080/webhook \
		-H "Content-Type: application/json" \
		-d '{"event_name": "user.created", "organization_id": "org-123", "payload": {"user_id": "user-abc", "email": "test@example.com"}}' \
		-w "\n"
	@echo ""
	@echo "Sending test webhook (order.placed for org-456)..."
	@curl -X POST http://localhost:18080/webhook \
		-H "Content-Type: application/json" \
		-d '{"event_name": "order.placed", "organization_id": "org-456", "payload": {"order_id": "order-xyz", "amount": 99.99}}' \
		-w "\n"
	@echo ""
	@echo "✅ Webhooks sent! View the logs!"

list-tasks:
	@echo "Listing recent tasks (last 20)..."
	@docker-compose exec -T postgres psql -U absurd -d absurd -c "\
		SELECT \
			t.task_id, \
			r.run_id, \
			t.task_name, \
			t.state as status, \
			r.attempt, \
			t.max_attempts, \
			r.created_at, \
			COALESCE(r.completed_at, r.failed_at, r.started_at, r.created_at) AS updated_at \
		FROM absurd.t_notifications t \
		LEFT JOIN absurd.r_notifications r ON r.run_id = t.last_attempt_run \
		ORDER BY updated_at DESC NULLS LAST \
		LIMIT 20;"

dump-task:
	@if [ -z "$(TASK_ID)" ]; then echo "Usage: make dump-task TASK_ID=xxx"; exit 1; fi
	@echo "Dumping details for task $(TASK_ID)..."
	@echo ""
	@echo "=== Task ==="
	@docker-compose exec -T postgres psql -U absurd -d absurd -c "SELECT * FROM absurd.t_notifications WHERE task_id = '$(TASK_ID)';"
	@echo ""
	@echo "=== Runs ==="
	@docker-compose exec -T postgres psql -U absurd -d absurd -c "SELECT * FROM absurd.r_notifications WHERE task_id = '$(TASK_ID)' ORDER BY attempt;"
	@echo ""
	@echo "=== Checkpoints ==="
	@docker-compose exec -T postgres psql -U absurd -d absurd -c "SELECT checkpoint_name, status, owner_run_id, updated_at FROM absurd.c_notifications WHERE task_id = '$(TASK_ID)' ORDER BY updated_at;"

db-status:
	@echo "Checking database status..."
	@echo ""
	@echo "=== Schemas ==="
	@docker-compose exec -T postgres psql -U absurd -d absurd -c "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'absurd';" 2>/dev/null || echo "❌ Absurd schema not found"
	@echo ""
	@echo "=== Functions ==="
	@docker-compose exec -T postgres psql -U absurd -d absurd -c "SELECT routine_name FROM information_schema.routines WHERE routine_schema = 'absurd' AND routine_name = 'create_queue';" 2>/dev/null || echo "❌ create_queue function not found"
	@echo ""
	@echo "=== Queues ==="
	@docker-compose exec -T postgres psql -U absurd -d absurd -c "SELECT * FROM absurd.queues;" 2>/dev/null || echo "❌ Queues table not found - run 'make db-init'"
	@echo ""
	@echo "=== Queue Tables (notifications) ==="
	@docker-compose exec -T postgres psql -U absurd -d absurd -c "SELECT tablename FROM pg_tables WHERE schemaname = 'absurd' AND tablename LIKE '%notifications%' ORDER BY tablename;" 2>/dev/null || echo "❌ Queue tables not found"

db-init:
	@echo "Initializing Absurd database schema..."
	@echo "This will run the migration and create the queue."
	@echo ""
	@echo "Step 1: Running absurd.sql migration..."
	@docker-compose exec -T postgres psql -U absurd -d absurd < ../migrations/absurd.sql
	@echo "✓ Schema and functions created"
	@echo ""
	@echo "Step 2: Creating notifications queue..."
	@$(MAKE) create-queue
	@echo ""
	@echo "✓ Database initialized successfully"
	@echo ""
	@$(MAKE) db-status

db-reset:
	@echo "⚠️  WARNING: This will delete ALL data in the database!"
	@echo "Press Ctrl+C to cancel, or press Enter to continue..."
	@read confirm
	@echo "Resetting database..."
	docker-compose down -v
	docker-compose up -d postgres
	@echo "Waiting for PostgreSQL to start..."
	@sleep 8
	@$(MAKE) db-init
	@echo "✓ Database reset complete"

db-shell:
	@echo "Opening psql shell (ctrl+d or \q to exit)..."
	@docker-compose exec postgres psql -U absurd -d absurd

db-connect:
	@echo ""
	@echo "==================================="
	@echo "Database Connection Information"
	@echo "==================================="
	@echo ""
	@echo "From host machine:"
	@echo "  psql 'postgres://absurd:absurd@localhost:15432/absurd?sslmode=disable'"
	@echo ""
	@echo "From inside Docker network:"
	@echo "  postgres://absurd:absurd@postgres:5432/absurd?sslmode=disable"
	@echo ""
	@echo "Connection details:"
	@echo "  Host:     localhost (or postgres from containers)"
	@echo "  Port:     15432 (host) / 5432 (container)"
	@echo "  Database: absurd"
	@echo "  User:     absurd"
	@echo "  Password: absurd"
	@echo ""
	@echo "Quick queries:"
	@echo "  make db-shell                    - Open interactive psql"
	@echo "  make list-tasks                  - List recent tasks"
	@echo "  make dump-task TASK_ID=xxx       - Show task details"
	@echo "==================================="

clean:
	@echo "Stopping and removing all containers and volumes..."
	docker-compose down -v
	@echo "Removing binary..."
	rm -f $(BINARY_NAME)
	@echo "✓ Cleanup complete"
