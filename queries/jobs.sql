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
ORDER BY id DESC
LIMIT $1 OFFSET $2;

-- name: ListJobsByStatus :many
SELECT * FROM jobs
WHERE status = $1
ORDER BY id DESC
LIMIT $2 OFFSET $3;

-- name: CancelJob :one
DELETE FROM jobs
WHERE id = $1 AND status = 'queued'
RETURNING *;
