ALTER TABLE query_jobs
  ADD COLUMN user_id BIGINT UNSIGNED NULL
    COMMENT 'NULL = pre-auth legacy rows; application layer enforces non-NULL for new rows',
  ADD INDEX idx_query_jobs_user_id (user_id),
  ADD CONSTRAINT fk_query_jobs_user_id
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT;
