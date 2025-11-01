#!/bin/bash

API_URL="${API_URL:-http://localhost:18080}"
set -x
echo "==================================="
echo "Testing Absurd Notification Router"
echo "API: $API_URL"
echo "==================================="
echo ""

# Test 1: user.created for org-123 (should match 2 destinations)
echo "📨 Test 1: user.created event for org-123"
curl -X POST "$API_URL/webhook" \
  -H "Content-Type: application/json" \
  -d '{
    "event_name": "user.created",
    "organization_id": "org-123",
    "payload": {
      "user_id": "u-001",
      "email": "alice@example.com",
      "name": "Alice Johnson"
    }
  }' \
  -w "\n\n"

sleep 1

# Test 2: order.placed for org-123 (should match 1 destination)
echo "📦 Test 2: order.placed event for org-123"
curl -X POST "$API_URL/webhook" \
  -H "Content-Type: application/json" \
  -d '{
    "event_name": "order.placed",
    "organization_id": "org-123",
    "payload": {
      "order_id": "ord-12345",
      "amount": 199.99,
      "currency": "USD"
    }
  }' \
  -w "\n\n"

sleep 1

# Test 3: payment.completed for org-456 (should match 1 destination)
echo "💳 Test 3: payment.completed event for org-456"
curl -X POST "$API_URL/webhook" \
  -H "Content-Type: application/json" \
  -d '{
    "event_name": "payment.completed",
    "organization_id": "org-456",
    "payload": {
      "payment_id": "pay-67890",
      "amount": 49.99,
      "method": "credit_card"
    }
  }' \
  -w "\n\n"

sleep 1

# Test 4: unknown event (should succeed but not create any send tasks)
echo "❓ Test 4: unknown event for org-999"
curl -X POST "$API_URL/webhook" \
  -H "Content-Type: application/json" \
  -d '{
    "event_name": "unknown.event",
    "organization_id": "org-999",
    "payload": {
      "test": "data"
    }
  }' \
  -w "\n\n"

echo "==================================="
echo "✅ All test webhooks sent!"
echo ""
echo "Check the results:"
echo "  make logs"
echo "  make list-tasks"
echo "==================================="
