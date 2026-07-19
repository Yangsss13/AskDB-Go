ALTER TABLE query_jobs
  DROP COLUMN next_retry_at,
  DROP COLUMN attempt_count;
