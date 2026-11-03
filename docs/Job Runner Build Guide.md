# Job Runner: Build Guide

Project 1 of 5 in the Go backend series. Goal: learn goroutines, channels, worker pools, PostgreSQL, retries and graceful shutdown by building an async job processing service.

## 1. What you're building

A service with an HTTP API where clients submit jobs. Jobs are stored in PostgreSQL, picked up by a pool of background workers, executed, and tracked through a lifecycle. Failed jobs are retried with backoff, and jobs that keep failing end up in a dead state you can inspect.

No frontend. You interact with it through `curl` and tests.

### What you will learn

- Goroutines, channels, `sync.WaitGroup`, and `context` cancellation
- Worker pool design and backpressure
- Using PostgreSQL as a job queue (`FOR UPDATE SKIP LOCKED`)
- Retry with exponential backoff and jitter
- Graceful shutdown (finish in-flight work, don't lose jobs)
- Crash recovery (a worker dies mid-job)
- Writing integration tests against a real database

### Out of scope for the MVP

Scheduling cron jobs, priorities, multiple queues, authentication, a UI, and horizontal scaling across many machines. Some of these are stretch goals at the end.

## 2. Architecture

One Go binary with three cooperating parts:

1. **API server (Gin):** validates requests, inserts jobs, serves status.
2. **Dispatcher:** a single goroutine that polls Postgres for ready jobs, claims them, and pushes them onto a channel.
3. **Worker pool:** N goroutines reading from that channel, executing the job handler, and writing the result back.

PostgreSQL is the single source of truth. The channel is only a hand-off buffer. If the process dies, nothing is lost because job state lives in the database.

```text
client -> API -> [jobs table] <- dispatcher -> channel -> workers -> [jobs table]
```

Keep the job handlers simple. Two or three fake job types are enough, for example `echo`, `sleep` (sleeps N seconds), and `fail_randomly` (fails a percentage of the time so you can test retries).

## 3. Data model

Start with one table.

```sql
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

CREATE INDEX jobs_ready_idx ON jobs (run_at) WHERE status = 'queued';
```

Status meanings:

- `queued`: waiting to run (including waiting for a retry, via a future `run_at`)
- `running`: claimed by a worker
- `succeeded`: done
- `failed`: last attempt failed but retries remain (you can also just move it back to `queued`; pick one approach and stay consistent)
- `dead`: exhausted `max_attempts`

Optional later: a `job_attempts` table that logs each attempt with start time, end time and error. It's great for debugging and a nice thing to show in the README.

## 4. API design

| Method | Path | Purpose |
| --- | --- | --- |
| POST | `/jobs` | Create a job (`type`, `payload`, optional `max_attempts`, optional `run_at`) |
| GET | `/jobs/:id` | Get status, attempts, last error |
| GET | `/jobs` | List jobs, filter by `status` and `type`, with pagination |
| POST | `/jobs/:id/retry` | Manually requeue a `dead` job |
| DELETE | `/jobs/:id` | Cancel a job that is still `queued` |
| GET | `/healthz` | Liveness and DB connectivity |

Return `202 Accepted` on job creation. Use consistent JSON error bodies. Validate that `type` is a registered handler and reject unknown types with `400`.

## 5. Milestones

Treat each milestone as a commit-worthy checkpoint. Don't move on until the milestone's checks pass.

### Milestone 1: Foundations (day 1)

- Initialize the module, folder layout, and `docker-compose.yml` with Postgres.
- Set up migrations (golang-migrate or goose) and write the schema above.
- Set up SQLC: write queries for insert, get by id, and list.
- Load config from environment variables (port, DB URL, worker count, poll interval).
- Add structured logging with `log/slog` from day one.

**Check:** `docker compose up`, run migrations, and insert a row through a Go test.

### Milestone 2: API without workers (day 1-2)

- Implement `POST /jobs`, `GET /jobs/:id`, `GET /jobs`.
- Validate input, return proper status codes, add a `/healthz` endpoint.
- Write handler tests using `httptest`.

**Check:** you can create and read jobs with `curl`. They sit in `queued` forever. That's expected.

### Milestone 3: Claiming jobs safely (day 2-3)

This is the most important database concept in the project. Write one SQL query that atomically claims a job:

```sql
UPDATE jobs
SET status = 'running', locked_at = now(), locked_by = $1,
    attempts = attempts + 1, updated_at = now()
WHERE id = (
  SELECT id FROM jobs
  WHERE status = 'queued' AND run_at <= now()
  ORDER BY run_at
  FOR UPDATE SKIP LOCKED
  LIMIT 1
)
RETURNING *;
```

Why this matters: with `SKIP LOCKED`, many workers (or many instances of your service) can claim jobs concurrently and never grab the same row. Make sure you understand why a plain `SELECT` followed by an `UPDATE` would be a race condition.

**Check:** write a test that inserts 100 jobs, runs 10 goroutines claiming concurrently, and asserts every job was claimed exactly once.

### Milestone 4: Worker pool (day 3-4)

- Define a handler interface, for example `type Handler func(ctx context.Context, payload []byte) error`, and a registry map from job type to handler.
- Build the dispatcher: loop, claim a job, send to a buffered channel. If no job is found, sleep for the poll interval. If the channel is full, the send blocks, which gives you natural backpressure.
- Start N worker goroutines that read from the channel, run the handler with a per-job timeout (`context.WithTimeout`), and then mark the job `succeeded` or handle the failure.
- Recover from panics inside handlers so one bad job can't kill a worker.

**Check:** submit 50 `sleep` jobs and watch them process with N at a time in the logs.

### Milestone 5: Retries and dead jobs (day 4-5)

- On failure, store `last_error`. If `attempts < max_attempts`, set status back to `queued` with `run_at = now() + backoff`. Otherwise mark `dead`.
- Backoff formula: `base * 2^attempts`, capped at a maximum, plus random jitter. Write it as a small pure function so it's easy to unit test.
- Add `POST /jobs/:id/retry` for dead jobs.

**Check:** a `fail_randomly` job eventually succeeds, and an `always_fail` job ends up `dead` after exactly `max_attempts` attempts.

### Milestone 6: Graceful shutdown and crash recovery (day 5-6)

Graceful shutdown:

1. Catch `SIGINT` and `SIGTERM` with `signal.NotifyContext`.
2. Stop accepting new HTTP requests (`server.Shutdown(ctx)`).
3. Stop the dispatcher from claiming new jobs.
4. Close the channel and let workers finish in-flight jobs (`WaitGroup`), with an overall deadline (for example 30 seconds).
5. Anything still running after the deadline should be released back to `queued` or left for the recovery process below.

Crash recovery (the "visibility timeout" idea):

- If the process is killed with `kill -9`, jobs stay `running` forever. Add a reaper goroutine that periodically finds jobs where `status = 'running'` and `locked_at` is older than a threshold, and requeues them.
- Discuss in your README what this means: a job might run twice, so handlers should be idempotent. This is called at-least-once delivery, and you'll meet it again in HookRelay and EventBridge.

**Check:** start 20 slow jobs, send `SIGTERM`, and confirm in-flight jobs finish and nothing is lost. Then repeat with `kill -9` and confirm the reaper recovers the stuck jobs.

### Milestone 7: Tests, polish, documentation (day 6+)

- Integration tests against a real Postgres (testcontainers-go, or the compose database with a separate test schema).
- Run the test suite with `-race`. Fix every data race it reports.
- A `Makefile` with `up`, `migrate`, `test`, `run`, `lint`.
- README with an architecture diagram, curl examples, design decisions, and a "what happens when things fail" section.
- Optional: a simple load test with `hey` or `k6` (1,000 jobs, 10 workers).

## 6. Suggested project layout

```text
jobrunner/
  cmd/jobrunner/main.go        # wiring, config, signal handling
  internal/
    api/                       # Gin handlers, request/response types
    config/                    # env loading
    jobs/                      # domain types, status constants
    queue/                     # dispatcher, claim logic
    worker/                    # pool, handler registry, retry logic
    store/                     # SQLC generated code + wrappers
  migrations/
  queries/                     # SQLC .sql files
  docker-compose.yml
  Makefile
  README.md
```

Keep `main.go` thin. Pass dependencies explicitly (constructors that take a store, a logger, and config) instead of using globals. This makes testing much easier.

## 7. Concepts to understand before you move on

Ask yourself these at the end. If you can't answer one, go back and learn it.

1. Why is `SELECT` then `UPDATE` unsafe for claiming jobs, and how does `SKIP LOCKED` fix it?
2. What happens to a job if the process crashes right after claiming it?
3. Why does a buffered channel give you backpressure, and what happens if it's unbuffered?
4. Why add jitter to backoff?
5. What is the difference between `context` cancellation and the `WaitGroup` in shutdown?
6. Why must job handlers be idempotent?
7. What does the `-race` flag find, and what bugs did it find in your code?

## 8. Common pitfalls

- **Goroutine leaks:** every goroutine you start needs a clear way to stop. Pass a `context` to all of them.
- **Forgetting the per-job timeout:** one hung handler can block a worker forever.
- **Updating job state outside a transaction** when multiple fields must change together.
- **Using `time.Sleep` in loops** instead of `select` with `ctx.Done()` and a ticker, which makes shutdown slow.
- **Closing a channel from the wrong goroutine.** Only the sender should close it.
- **Ignoring errors from the final status update.** If marking `succeeded` fails, the job will rerun. Log it loudly.
- **Over-engineering early.** Get the simple single-instance version working end to end before adding anything from the stretch list.

## 9. Definition of done

The project is finished when all of these are true:

- [ ] Jobs can be created, listed, fetched, retried and cancelled through the API
- [ ] Jobs are processed concurrently by a configurable worker pool
- [ ] Failed jobs retry with exponential backoff and jitter, then become `dead`
- [ ] `SIGTERM` drains in-flight jobs without losing any
- [ ] Jobs stuck after a hard crash are recovered automatically
- [ ] Tests pass with `-race`, including a concurrent-claim test
- [ ] The whole thing runs with a single `docker compose up`
- [ ] README has an architecture diagram, curl examples, and a failure-modes section

## 10. Stretch goals

Pick one or two only after the checklist above is complete.

- Job priorities and multiple named queues
- Scheduled jobs and a simple cron-like recurring job
- A `job_attempts` history table and an endpoint to view it
- Unique jobs (an idempotency key so the same job can't be enqueued twice)
- Prometheus metrics: queue depth, job duration, success and failure counters (this becomes useful later in ServicePulse)
- Run two instances at once and prove that `SKIP LOCKED` keeps them from colliding
- Use Postgres `LISTEN/NOTIFY` so the dispatcher wakes up instantly instead of polling

## 11. Suggested first day

1. Create the repo and folder layout.
2. Write `docker-compose.yml` with Postgres and confirm you can connect.
3. Write the first migration and run it.
4. Set up SQLC and generate code for `InsertJob` and `GetJob`.
5. Write a test that inserts and reads one job.
6. Commit. You now have a foundation, and everything else builds on it.
