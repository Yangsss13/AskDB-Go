-- Phase 7: add retry tracking columns to query_jobs.
-- attempt_count counts how many times the job has been retried (0 = never).
-- next_retry_at is set when transitioning to the retrying state.
ALTER TABLE query_jobs
  ADD COLUMN attempt_count TINYINT UNSIGNED NOT NULL DEFAULT 0 AFTER data_source_id,
  ADD COLUMN next_retry_at DATETIME(3)      NULL               AFTER attempt_count;
