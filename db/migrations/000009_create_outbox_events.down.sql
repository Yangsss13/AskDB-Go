-- Phase 8 rollback: drop outbox_events (FK on query_jobs must be dropped first).
DROP TABLE IF EXISTS outbox_events;
