package telemetry

import (
	"sync"
	"testing"
	"time"
)

func TestTracker_WorkerStateTransitions(t *testing.T) {
	tracker := NewTracker(3, 10)

	snap := tracker.Snapshot()
	if len(snap.Workers) != 3 {
		t.Fatalf("expected 3 workers, got %d", len(snap.Workers))
	}
	if snap.Workers[0].Status != "idle" {
		t.Fatalf("expected idle, got %s", snap.Workers[0].Status)
	}

	// Mark worker-01 as busy
	tracker.SetWorkerBusy("worker-01", 101, "echo", 1, 5)

	snap = tracker.Snapshot()
	var w1 *WorkerState
	for i := range snap.Workers {
		if snap.Workers[i].ID == "worker-01" {
			w1 = &snap.Workers[i]
			break
		}
	}

	if w1 == nil {
		t.Fatal("worker-01 not found")
	}
	if w1.Status != "busy" {
		t.Errorf("expected busy, got %s", w1.Status)
	}
	if w1.JobID == nil || *w1.JobID != 101 {
		t.Errorf("expected job id 101, got %v", w1.JobID)
	}
	if w1.JobType != "echo" {
		t.Errorf("expected job type echo, got %s", w1.JobType)
	}

	// Set back to idle
	tracker.SetWorkerIdle("worker-01")
	snap = tracker.Snapshot()
	for i := range snap.Workers {
		if snap.Workers[i].ID == "worker-01" {
			w1 = &snap.Workers[i]
			break
		}
	}
	if w1.Status != "idle" {
		t.Errorf("expected idle, got %s", w1.Status)
	}
	if w1.JobID != nil {
		t.Errorf("expected nil job id, got %v", w1.JobID)
	}
}

func TestBroadcaster_FanOut(t *testing.T) {
	broadcaster := NewBroadcaster()

	ch1, unsub1 := broadcaster.Subscribe()
	defer unsub1()

	ch2, unsub2 := broadcaster.Subscribe()
	defer unsub2()

	ev := DashboardEvent{
		Type:      "test_event",
		JobID:     42,
		Timestamp: time.Now(),
	}

	broadcaster.Broadcast(ev)

	select {
	case received := <-ch1:
		if received.JobID != 42 {
			t.Errorf("ch1 expected 42, got %d", received.JobID)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("ch1 timed out waiting for event")
	}

	select {
	case received := <-ch2:
		if received.JobID != 42 {
			t.Errorf("ch2 expected 42, got %d", received.JobID)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("ch2 timed out waiting for event")
	}
}

func TestTracker_ConcurrentSafety(t *testing.T) {
	tracker := NewTracker(5, 10)
	ch, unsub := tracker.Subscribe()
	defer unsub()

	var wg sync.WaitGroup

	// Concurrently update workers and buffer
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			wID := "worker-00"
			tracker.SetWorkerBusy(wID, int64(id), "sleep", 1, 3)
			tracker.RecordDispatch(int64(id), "sleep", id%10, 10)
			tracker.RecordSuccess(int64(id), wID, 10*time.Millisecond)
			tracker.SetWorkerIdle(wID)
		}(i)
	}

	// Concurrently read snapshots
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = tracker.Snapshot()
		}()
	}

	wg.Wait()

	// Drain any received events without blocking
	drained := 0
	for {
		select {
		case <-ch:
			drained++
		default:
			goto DONE
		}
	}
DONE:
	if drained == 0 {
		t.Error("expected to have received events from broadcaster")
	}
}
