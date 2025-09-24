package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"queue-service/internal/repo"
	"queue-service/internal/service"
)

// Валидация и вызов в сервис
func EnqueueHandler(s *service.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var t repo.Task
		if err := json.NewDecoder(r.Body).Decode(&t); err != nil {
			http.Error(w, "invalid json, try: \n {\n\t\"id\":\"<string>\",\n\t\"payload\":\"<string>\",\n\t\"max_retries\":<int>\n}", http.StatusBadRequest)
			return
		}
		if t.ID == "" {
			http.Error(w, "id required", http.StatusBadRequest)
			return
		}
		if t.MaxRetries < 0 {
			http.Error(w, "max_retries must be >= 0", http.StatusBadRequest)
			return
		}
		if err := s.Enqueue(t); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		fmt.Fprintf(w, "enqueued")
	}
}

// Отдаем healthcheck
func HealthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "ok")
}

func DebugDumpHandler(r *repo.Repo) http.HandlerFunc {
	return func(w http.ResponseWriter, r2 *http.Request) {
		all := r.DumpAll()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(all)
	}
}
