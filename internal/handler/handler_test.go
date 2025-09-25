package handler

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"queue-service/internal/repo"
	"queue-service/internal/service"
	"testing"
)

func newTestService() *service.Service {
	r := repo.NewRepo()
	s := service.NewService(r, 1, 1)
	s.Start()
	return s
}

func TestEnqueueHandler_Success(t *testing.T) {
	s := newTestService()
	defer s.Shutdown()

	h := EnqueueHandler(s)
	body := bytes.NewBufferString(`{"id":"task1","payload":"data","max_retries":1}`)

	req := httptest.NewRequest(http.MethodPost, "/enqueue", body)
	w := httptest.NewRecorder()

	h(w, req)

	if w.Result().StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", w.Result().StatusCode)
	}
}

func TestEnqueueHandler_InvalidJSON(t *testing.T) {
	s := newTestService()
	defer s.Shutdown()

	h := EnqueueHandler(s)
	body := bytes.NewBufferString(`{invalid}`)

	req := httptest.NewRequest(http.MethodPost, "/enqueue", body)
	w := httptest.NewRecorder()

	h(w, req)

	if w.Result().StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Result().StatusCode)
	}
}

func TestEnqueueHandler_EmptyID(t *testing.T) {
	s := newTestService()
	defer s.Shutdown()

	h := EnqueueHandler(s)
	body := bytes.NewBufferString(`{"id":"","payload":"test_data","max_retries":1}`)

	req := httptest.NewRequest(http.MethodPost, "/enqueue", body)
	w := httptest.NewRecorder()

	h(w, req)

	if w.Result().StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Result().StatusCode)
	}
}

func TestEnqueueHandler_NegativeRetries(t *testing.T) {
	s := newTestService()
	defer s.Shutdown()

	h := EnqueueHandler(s)
	body := bytes.NewBufferString(`{"id":"task1","payload":"test_data","max_retries":-1}`)

	req := httptest.NewRequest(http.MethodPost, "/enqueue", body)
	w := httptest.NewRecorder()

	h(w, req)

	if w.Result().StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Result().StatusCode)
	}
}

func TestEnqueueHandler_QueueFull(t *testing.T) {
	s := service.NewService(repo.NewRepo(), 0, 1)
	s.Start()
	defer s.Shutdown()

	h := EnqueueHandler(s)

	// Очередь станет заполнена
	req1 := httptest.NewRequest(http.MethodPost, "/enqueue", bytes.NewBufferString(`{"id":"task1","payload":"test_data","max_retries":1}`))
	w1 := httptest.NewRecorder()
	h(w1, req1)

	// Переполнение
	req2 := httptest.NewRequest(http.MethodPost, "/enqueue", bytes.NewBufferString(`{"id":"t2","payload":"test_data","max_retries":1}`))
	w2 := httptest.NewRecorder()
	h(w2, req2)

	if w2.Result().StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", w2.Result().StatusCode)
	}
}

func TestHealthHandler(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()

	HealthHandler(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Result().StatusCode)
	}
}

func TestDebugDumpHandler(t *testing.T) {
	r := repo.NewRepo()
	r.CreateOrUpdate(repo.Task{ID: "task1"}, "queued")

	h := DebugDumpHandler(r)
	req := httptest.NewRequest(http.MethodGet, "/debug/dump", nil)
	w := httptest.NewRecorder()

	h(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Result().StatusCode)
	}

	var data map[string]repo.TaskMeta
	if err := json.NewDecoder(w.Body).Decode(&data); err != nil {
		t.Fatalf("invalid json: %v", err)
	}

	if _, ok := data["task1"]; !ok {
		t.Fatalf("expected task task1 in dump, got %+v", data)
	}
}
