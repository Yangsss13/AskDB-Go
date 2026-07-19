-- Phase 8: Transactional Outbox for reliable RabbitMQ publishing.
-- publish failures retry via capped exponential backoff; no permanent failed status.
CREATE TABLE IF NOT EXISTS outbox_events (
  id               BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT,
  message_id       VARCHAR(64)      NOT NULL,
  event_type       VARCHAR(64)      NOT NULL,
  version          TINYINT UNSIGNED NOT NULL DEFAULT 1,
  job_id           BIGINT UNSIGNED  NOT NULL,
  payload          JSON             NOT NULL,
  status           ENUM('pending','publishing','published') NOT NULL DEFAULT 'pending',
  attempt_count    INT UNSIGNED     NOT NULL DEFAULT 0,
  next_retry_at    DATETIME(3)      NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  lease_token      VARCHAR(64)      NULL,
  lease_expires_at DATETIME(3)      NULL,
  last_error       VARCHAR(512)     NULL,
  created_at       DATETIME(3)      NOT NULL,
  published_at     DATETIME(3)      NULL,
  occurred_at      DATETIME(3)      NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_ob_message_id (message_id),
  UNIQUE KEY uq_ob_type_job (event_type, job_id),
  CONSTRAINT fk_ob_job FOREIGN KEY (job_id) REFERENCES query_jobs(id) ON DELETE RESTRICT,
  INDEX idx_ob_dispatch (status, next_retry_at),
  -- covers the lease-expiry takeover path: status='publishing' AND lease_expires_at <= ?
  INDEX idx_ob_lease (status, lease_expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
