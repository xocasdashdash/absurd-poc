-- Create destinations table for notification routing
CREATE TABLE IF NOT EXISTS notification_destinations (
    id SERIAL PRIMARY KEY,
    organization_id TEXT NOT NULL,
    event_name TEXT NOT NULL,
    webhook_url TEXT NOT NULL,
    enabled BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(organization_id, event_name, webhook_url)
);

-- Create index for fast lookups
CREATE INDEX idx_destinations_lookup ON notification_destinations(organization_id, event_name) WHERE enabled = true;

-- Insert sample destinations for testing (pointing to local webhook.site container)
-- The webhook.site container will capture these webhooks
INSERT INTO notification_destinations (organization_id, event_name, webhook_url) VALUES
    ('org-123', 'user.created', 'http://webhook-site:5678/abcdef-12345-abcdef-12345-abcdef'),
    ('org-123', 'user.created', 'http://webhook-site:5678/abcdef-12345-abcdef-12345-abcdef'),
    ('org-123', 'order.placed', 'http://webhook-site:5678/abcdef-12345-abcdef-12345-abcdef'),
    ('org-456', 'user.created', 'http://webhook-site:5678/abcdef-12345-abcdef-12345-abcdef'),
    ('org-456', 'order.placed', 'http://webhook-site:5678/abcdef-12345-abcdef-12345-abcdef')
ON CONFLICT (organization_id, event_name, webhook_url) DO NOTHING;
