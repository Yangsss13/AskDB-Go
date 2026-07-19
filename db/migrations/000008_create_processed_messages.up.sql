-- Phase 7: idempotency log for RabbitMQ consumer.
-- Each message_id maps to exactly one processing record.
-- The (message_type, job_id) unique constraint prevents a second message from
-- the same logical job from being processed concurrently.
CREATE TABLE IF NOT EXISTS processed_messages (
  message_id       VARCHAR(64)   NOT NULL,
  message_type     VARCHAR(64)   NOT NULL,
  job_id           BIGINT UNSIGNED NOT NULL,
  attempt          TINYINT UNSIGNED NOT NULL DEFAULT 0,
  status           ENUM('processing','retry_scheduled','completed') NOT NULL DEFAULT 'processing',
  lease_token      VARCHAR(64)   NOT NULL,
  lease_expires_at DATETIME(3)   NOT NULL,
  completed_at     DATETIME(3)   NULL,
  PRIMARY KEY (message_id),
  UNIQUE INDEX uq_pm_type_job (message_type, job_id),
  INDEX idx_pm_job_id (job_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
