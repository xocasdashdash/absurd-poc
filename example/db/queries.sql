-- ============================================
-- Useful SQL Queries for Absurd Example
-- ============================================
-- Run these in: make db-shell

-- ============================================
-- TASK MONITORING
-- ============================================
-- NOTE: Replace 'notifications' with your queue name in all queries below

-- List all recent tasks
SELECT 
    t.task_id, 
    r.run_id,
    t.task_name, 
    t.state as status, 
    r.attempt,
    t.max_attempts,
    t.created_at,
    COALESCE(r.completed_at, r.failed_at, r.started_at, r.created_at) as updated_at
FROM absurd.t_notifications t
LEFT JOIN absurd.r_notifications r ON r.run_id = t.last_attempt_run
ORDER BY updated_at DESC NULLS LAST
LIMIT 20;

-- Count tasks by status
SELECT 
    state as status, 
    COUNT(*) as count 
FROM absurd.t_notifications
GROUP BY state;

-- Count tasks by type
SELECT 
    t.task_name, 
    COUNT(*) as count,
    AVG(EXTRACT(EPOCH FROM (r.completed_at - r.started_at))) as avg_duration_seconds
FROM absurd.t_notifications t
JOIN absurd.r_notifications r ON r.run_id = t.last_attempt_run
WHERE t.state = 'completed' AND r.completed_at IS NOT NULL
GROUP BY t.task_name;

-- Find failed tasks
SELECT 
    t.task_id,
    t.task_name,
    t.state as status,
    r.attempt,
    t.max_attempts,
    r.failure_reason as error,
    t.created_at
FROM absurd.t_notifications t
JOIN absurd.r_notifications r ON r.run_id = t.last_attempt_run
WHERE t.state = 'failed' 
ORDER BY t.created_at DESC;

-- Find running tasks
SELECT 
    t.task_id,
    t.task_name,
    r.state as status,
    r.attempt,
    r.claimed_by,
    r.claim_timeout,
    t.created_at,
    NOW() - r.started_at as running_for
FROM absurd.t_notifications t
JOIN absurd.r_notifications r ON r.run_id = t.last_attempt_run
WHERE r.state = 'running' 
ORDER BY t.created_at DESC;

-- Find stuck tasks (claimed but timed out)
SELECT 
    t.task_id,
    t.task_name,
    r.claimed_by,
    r.claim_timeout,
    NOW() - r.claim_timeout as overdue_by
FROM absurd.t_notifications t
JOIN absurd.r_notifications r ON r.run_id = t.last_attempt_run
WHERE r.state = 'running' 
    AND r.claim_timeout < NOW()
ORDER BY r.claim_timeout;

-- ============================================
-- TASK DETAILS
-- ============================================

-- Get full task details (replace with actual task_id)
SELECT * FROM absurd.t_notifications WHERE task_id = 'TASK_ID_HERE' \gx

-- Get task runs
SELECT * FROM absurd.r_notifications WHERE task_id = 'TASK_ID_HERE' ORDER BY created_at DESC;

-- Get task checkpoints
SELECT 
    task_id,
    owner_run_id as run_id,
    checkpoint_name,
    updated_at
FROM absurd.c_notifications 
WHERE task_id = 'TASK_ID_HERE' 
ORDER BY updated_at DESC;

-- Get task parameters (JSON)
SELECT 
    task_id,
    task_name,
    params::text as parameters
FROM absurd.t_notifications 
WHERE task_id = 'TASK_ID_HERE';

-- ============================================
-- PERFORMANCE ANALYSIS
-- ============================================

-- Average task duration by type
SELECT 
    t.task_name,
    COUNT(*) as total_tasks,
    AVG(EXTRACT(EPOCH FROM (r.completed_at - r.started_at))) as avg_seconds,
    MIN(EXTRACT(EPOCH FROM (r.completed_at - r.started_at))) as min_seconds,
    MAX(EXTRACT(EPOCH FROM (r.completed_at - r.started_at))) as max_seconds
FROM absurd.t_notifications t
JOIN absurd.r_notifications r ON r.run_id = t.last_attempt_run
WHERE t.state = 'completed' AND r.completed_at IS NOT NULL AND r.started_at IS NOT NULL
GROUP BY t.task_name;

-- Tasks processed per hour (last 24 hours)
SELECT 
    DATE_TRUNC('hour', t.created_at) as hour,
    t.task_name,
    COUNT(*) as tasks_processed
FROM absurd.t_notifications t
WHERE t.created_at > NOW() - INTERVAL '24 hours'
GROUP BY hour, t.task_name
ORDER BY hour DESC;

-- Retry rate by task type
SELECT 
    t.task_name,
    COUNT(DISTINCT t.task_id) as total_tasks,
    SUM(CASE WHEN r.attempt > 1 THEN 1 ELSE 0 END) as retried_tasks,
    ROUND(100.0 * SUM(CASE WHEN r.attempt > 1 THEN 1 ELSE 0 END) / COUNT(DISTINCT t.task_id), 2) as retry_percentage
FROM absurd.t_notifications t
LEFT JOIN absurd.r_notifications r ON r.task_id = t.task_id
GROUP BY t.task_name;

-- ============================================
-- NOTIFICATION DESTINATIONS
-- ============================================

-- List all destinations
SELECT * FROM notification_destinations ORDER BY organization_id, event_name;

-- Count destinations by organization
SELECT 
    organization_id, 
    COUNT(*) as destination_count 
FROM notification_destinations 
GROUP BY organization_id;

-- Find destinations for specific event
SELECT * 
FROM notification_destinations 
WHERE event_name = 'user.created' 
    AND enabled = true;

-- Add a new destination
INSERT INTO notification_destinations (organization_id, event_name, webhook_url)
VALUES ('org-789', 'user.created', 'https://webhook.site/your-unique-id')
ON CONFLICT (organization_id, event_name, webhook_url) DO NOTHING;

-- Disable a destination
UPDATE notification_destinations 
SET enabled = false 
WHERE id = 1;

-- ============================================
-- QUEUE MONITORING
-- ============================================

-- View queue statistics
SELECT * FROM absurd.queues;

-- Count pending tasks in notifications queue
SELECT 
    'notifications' as queue_name,
    COUNT(*) as pending_tasks
FROM absurd.t_notifications
WHERE state = 'pending';

-- ============================================
-- CLEANUP
-- ============================================
-- Note: Use 'absurdctl cleanup' command instead for safer batch deletion

-- Delete old completed tasks (older than 7 days) - BE CAREFUL!
-- DELETE FROM absurd.t_notifications 
-- WHERE state = 'completed' 
--     AND created_at < NOW() - INTERVAL '7 days';

-- Delete old failed tasks (older than 30 days) - BE CAREFUL!
-- DELETE FROM absurd.t_notifications 
-- WHERE state = 'failed' 
--     AND created_at < NOW() - INTERVAL '30 days';

-- Clear all tasks (CAUTION! THIS WILL DELETE EVERYTHING!)
-- TRUNCATE absurd.t_notifications CASCADE;

-- ============================================
-- DEBUGGING
-- ============================================

-- Find tasks that failed with specific error
SELECT 
    t.task_id,
    t.task_name,
    r.failure_reason->>'message' as error,
    r.attempt,
    t.created_at
FROM absurd.t_notifications t
JOIN absurd.r_notifications r ON r.run_id = t.last_attempt_run
WHERE r.failure_reason->>'message' LIKE '%webhook endpoint returned error%'
ORDER BY t.created_at DESC;

-- View task execution timeline
SELECT 
    t.task_id,
    t.task_name,
    t.state as status,
    t.created_at as task_created,
    r.started_at as run_started,
    COALESCE(r.completed_at, r.failed_at) as run_finished,
    EXTRACT(EPOCH FROM (COALESCE(r.completed_at, r.failed_at) - r.started_at)) as execution_duration_seconds
FROM absurd.t_notifications t
LEFT JOIN absurd.r_notifications r ON r.run_id = t.last_attempt_run
WHERE t.task_id = 'TASK_ID_HERE';

-- View all events for a specific organization
SELECT 
    t.task_id,
    t.task_name,
    t.params->'event' as event_data,
    t.state as status,
    t.created_at
FROM absurd.t_notifications t
WHERE t.task_name = 'route-notification'
    AND t.params->'event'->>'organization_id' = 'org-123'
ORDER BY t.created_at DESC;
