-- +goose Up
-- +goose StatementBegin
CREATE TYPE job_status AS ENUM ('queued', 'running', 'succeeded', 'failed', 'dead');

CREATE TABLE jobs (
    id            BIGSERIAL PRIMARY KEY,
    type          TEXT        NOT NULL,
    payload       JSONB       NOT NULL DEFAULT '{}',
    status        job_status  NOT NULL DEFAULT 'queued',
    attempts      INT         NOT NULL DEFAULT 0,
    max_attempts  INT         NOT NULL DEFAULT 5,
    run_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_at     TIMESTAMPTZ,
    locked_by     TEXT,
    last_error    TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Partial index: only index jobs waiting to be processed.
-- This keeps the index small and fast even when millions of completed jobs exist.
CREATE INDEX jobs_ready_idx ON jobs (run_at) WHERE status = 'queued';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS jobs;
DROP TYPE IF EXISTS job_status;
-- +goose StatementEnd
