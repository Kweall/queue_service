package repo

import (
	"testing"
)

func TestCreateOrUpdate_NewTask(t *testing.T) {
	r := NewRepo()
	task := Task{ID: "test_task", Payload: "data", MaxRetries: 2}

	r.CreateOrUpdate(task, "queued")

	got, ok := r.Get("test_task")
	if !ok {
		t.Fatalf("task not found after CreateOrUpdate")
	}
	if got.State != "queued" {
		t.Errorf("expected state=queued, got %s", got.State)
	}
	if got.Attempts != 0 {
		t.Errorf("expected attempts=0, got %d", got.Attempts)
	}
}

func TestCreateOrUpdate_ResetAttempts(t *testing.T) {
	r := NewRepo()
	task := Task{ID: "test_task", Payload: "data", MaxRetries: 2}

	r.CreateOrUpdate(task, "queued")
	r.IncAttempts("test_task")
	r.IncAttempts("test_task")

	r.CreateOrUpdate(task, "queued")
	got, _ := r.Get("test_task")

	if got.Attempts != 0 {
		t.Errorf("expected attempts reset to 0, got %d", got.Attempts)
	}
}

func TestIncAttempts(t *testing.T) {
	r := NewRepo()
	task := Task{ID: "test_task"}
	r.CreateOrUpdate(task, "queued")

	if a := r.IncAttempts("test_task"); a != 1 {
		t.Errorf("expected 1, got %d", a)
	}
	if a := r.IncAttempts("test_task"); a != 2 {
		t.Errorf("expected 2, got %d", a)
	}
}
