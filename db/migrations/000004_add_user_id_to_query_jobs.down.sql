ALTER TABLE query_jobs
  DROP FOREIGN KEY fk_query_jobs_user_id,
  DROP INDEX idx_query_jobs_user_id,
  DROP COLUMN user_id;
