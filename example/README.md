# Absurd Notification Router Example

A complete example demonstrating the Absurd workflow engine with a real-world notification routing system.

## Overview

This example showcases how to use Absurd to build a reliable webhook notification router that:
- Receives webhook events via HTTP API
- Queries a database to find notification destinations
- Spawns background tasks to deliver webhooks with automatic retries
- Handles failures gracefully with exponential backoff

## Architecture

The example consists of:
- **Single Go Binary**: Can run as either API server or worker
- **PostgreSQL Database**: Stores Absurd tasks and notification destinations
- **Webhook.site**: Local echo server
- **Docker Compose**: Orchestrates all services

### Components

1. **API Mode** (`-mode api`)
   - HTTP server listening on port 8080
   - `/webhook` endpoint receives events
   - Spawns `route-notification` tasks

2. **Worker Mode** (`-mode worker`)
   - Processes two task types:
     - `route-notification`: Queries destinations and spawns webhook tasks
     - `send-webhook`: Sends HTTP POST to destination URLs
   - Automatic retries with exponential backoff


## Quick Start

### Prerequisites
- Go 1.25+
- Docker and Docker Compose
- Make (optional, but recommended)

### Setup

```bash
# Full setup (build + docker + start services)
make setup

# Or step by step:
make build           # Build Go binary
make docker-build    # Build Docker image
make up             # Start all services
```

### Test It

```bash
# Send test webhooks
make test-webhook

# View captured webhooks in your browser
open http://localhost:19090

# View logs
make logs

# List recent tasks
make list-tasks

# View details of a specific task
make dump-task TASK_ID=<task-id>
```

### Manual Testing

```bash
# Send a custom webhook
curl -X POST http://localhost:8080/webhook \
  -H "Content-Type: application/json" \
  -d '{
    "event_name": "user.created",
    "organization_id": "org-123",
    "payload": {
      "user_id": "u-12345",
      "email": "user@example.com"
    }
  }'
```

## Database Schema

### notification_destinations

Stores webhook destinations for routing events:

```sql
CREATE TABLE notification_destinations (
    id SERIAL PRIMARY KEY,
    organization_id TEXT NOT NULL,
    event_name TEXT NOT NULL,
    webhook_url TEXT NOT NULL,
    enabled BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(organization_id, event_name, webhook_url)
);
```

## Task Workflow

### 1. route-notification

**Input**: WebhookEvent (org_id, event_name, payload)

**Steps**:
1. `query-destinations`: Query DB for matching webhook URLs
2. `spawn-webhook-tasks`: Spawn `send-webhook` task for each destination

### 2. send-webhook

**Input**: DestinationURL, Event, DestinationID

**Steps**:
1. `http-post`: Send HTTP POST to destination URL

**Retry Strategy**: Exponential backoff (2s base, up to 60s, 5 max attempts)

## Development

### Local Development with go.work

The example uses a `go.work` file to reference the parent absurd module:

```go
go 1.25

use (
    .
    ..
)
```

This allows you to develop the example alongside changes to the core absurd library.

### Debugging with VS Code

Debug configurations are provided in `.vscode/launch.json`:

```bash
# 1. Start PostgreSQL
docker-compose up -d postgres
sleep 8
make create-queue

# 2. Open VS Code and press F5
# Choose "Debug Worker" or "Debug API"

# 3. Set breakpoints in main.go or absurd/client/client.go

# 4. Trigger task processing
make test-webhook
```

See [`.vscode/README.md`](.vscode/README.md) for detailed debugging instructions.

### Project Structure

```
example/
├── main.go              # Single binary (API + Worker modes)
├── go.mod              # Module definition
├── go.work             # Workspace for local development
├── Dockerfile          # Simple Alpine-based image
├── docker-compose.yml  # Service orchestration
├── Makefile            # Build and management commands
├── db/
│   └── init.sql       # Database initialization
└── README.md          # This file
```

## Makefile Commands

```bash
make help         # Show all available commands
make build        # Build Go binary (Linux AMD64)
make docker-build # Build Docker image with binary
make up           # Start all services
make down         # Stop all services
make logs         # View logs
make setup        # Full setup from scratch
make create-queue # Create the notifications queue
make test-webhook # Send test webhooks
make list-tasks   # List recent tasks
make dump-task    # Show task details (TASK_ID=xxx)
make db-shell     # Open interactive psql shell
make db-connect   # Show database connection info
make clean        # Remove containers, volumes, binary
```

### Database Access

**Interactive SQL shell:**
```bash
make db-shell
```

**Connection from host machine:**
```bash
psql 'postgres://absurd:absurd@localhost:15432/absurd?sslmode=disable'
```

**View connection info:**
```bash
make db-connect
```

## Cleanup

```bash
# Stop services and remove volumes
make clean
```

## Key Features Demonstrated

✅ **Single Binary Design**: One codebase, two modes  
✅ **Task Composition**: Tasks spawning other tasks  
✅ **Checkpointing**: Idempotent steps with state persistence  
✅ **Retry Logic**: Exponential backoff with configurable limits  
✅ **Database Integration**: PostgreSQL for task queue and application data  
✅ **Worker Concurrency**: Multiple workers processing tasks in parallel  
✅ **Graceful Shutdown**: Signal handling for clean termination  
✅ **Type Safety**: Generic task parameters and results  

## Troubleshooting

### "relation absurd.tasks does not exist"

The database schema hasn't been initialized. This happens if you're using an old Docker volume.

**Solution:**
```bash
# Check database status
make db-status

# If schema is missing, initialize it
make db-init

# Or reset everything (deletes all data)
make db-reset
```

### Binary build fails
```bash
# Ensure you're using Go 1.25+
go version

# Run from the example directory
cd example
make build
```

### Services won't start
```bash
# Check logs
make logs

# Restart from scratch
make clean
make setup
```

### Tasks not processing
```bash
# Check worker logs
docker-compose logs -f worker

# Verify database is initialized
make db-status

# Check for running tasks
make list-tasks

# View a specific task
make dump-task TASK_ID=xxx
```

### Database connection issues
```bash
# Check if PostgreSQL is running
docker-compose ps postgres

# View PostgreSQL logs
docker-compose logs postgres

# Test connection
make db-shell
```

## Learn More

- [Absurd Documentation](../README.md)
- [absurdctl CLI](../absurdctl)
- [Migration Files](../migrations/)

- [Absurd Workflows](https://lucumr.pocoo.org/2025/11/3/absurd-workflows/)
- [Indeed Worfklow Framework] (https://github.com/indeedeng/iwf)
