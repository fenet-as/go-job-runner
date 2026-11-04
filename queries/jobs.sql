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
