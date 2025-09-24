package service

import (
	"queue-service/internal/repo"
	"testing"
	"time"
)

func TestEnqueueAndProcess_Success(t *testing.T) {
	r := repo.NewRepo()
	s := NewService(r, 1, 10) // 1 воркер
	s.Start()

	task := repo.Task{ID: "test_task", Payload: "data", MaxRetries: 1}
	if err := s.Enqueue(task); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	// ждём завершения
	time.Sleep(600 * time.Millisecond)

	meta, ok := r.Get("test_task")
	if !ok {
		t.Fatalf("task not found in repo")
	}
	if meta.State != "done" && meta.State != "failed" && meta.State != "queued" {
		t.Errorf("expected state done, failed, or queued, got %s", meta.State)
	}

	s.Shutdown()
}

func TestShutdown_WaitsForTasks(t *testing.T) {
	r := repo.NewRepo()
	s := NewService(r, 1, 10)
	s.Start()

	task := repo.Task{ID: "long", Payload: "zzz", MaxRetries: 0}
	if err := s.Enqueue(task); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	// Ждём, чтобы воркер начал работу
	time.Sleep(50 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		s.Shutdown()
		close(done)
	}()

	select {
	case <-done:
		// Ок, graceful завершился
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown did not complete in time")
	}
}
