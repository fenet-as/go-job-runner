-- name: CreateJob :one
INSERT INTO jobs (
    type,
    payload,
    max_attempts,
    run_at
) VALUES (
    $1,
    $2,
    $3,
    $4
)
RETURNING *;

-- name: GetJobByID :one
SELECT * FROM jobs
WHERE id = $1;

-- name: ListJobs :many
SELECT * FROM jobs
WHERE (sqlc.narg('status')::job_status IS NULL OR status = sqlc.narg('status'))
  AND (sqlc.narg('type')::text IS NULL OR type = sqlc.narg('type'))
ORDER BY id DESC
LIMIT $1 OFFSET $2;

-- name: CancelJob :one
DELETE FROM jobs
WHERE id = $1 AND status = 'queued'
RETURNING *;

-- name: ClaimJob :one
UPDATE jobs
SET status = 'running',
    locked_at = now(),
    locked_by = sqlc.arg('worker_id')::text,
    attempts = attempts + 1,
    updated_at = now()
WHERE id = (
    SELECT id FROM jobs
    WHERE status = 'queued' AND run_at <= now()
    ORDER BY run_at
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING *;

-- name: CompleteJob :one
UPDATE jobs
SET status = 'succeeded',
    locked_at = NULL,
    locked_by = NULL,
    updated_at = now()
WHERE id = $1 AND status = 'running'
RETURNING *;

-- name: FailJob :one
UPDATE jobs
SET status = 'failed',
    last_error = sqlc.arg('last_error')::text,
    locked_at = NULL,
    locked_by = NULL,
    updated_at = now()
WHERE id = $1 AND status = 'running'
RETURNING *;
