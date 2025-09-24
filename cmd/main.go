package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"queue-service/internal/handler"
	"queue-service/internal/repo"
	"queue-service/internal/service"
	"strconv"
	"syscall"
	"time"
)

func mustEnvInt(name string, def int) int {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Printf("invalid %s=%s, using default %d", name, v, def)
		return def
	}
	return n
}

func main() {
	workers := mustEnvInt("WORKERS", 4)
	queueSize := mustEnvInt("QUEUE_SIZE", 64)

	repo := repo.NewRepo()
	svc := service.NewService(repo, workers, queueSize)
	svc.Start()

	mux := http.NewServeMux()
	mux.HandleFunc("/enqueue", handler.EnqueueHandler(svc))
	mux.HandleFunc("/healthz", handler.HealthHandler)
	mux.HandleFunc("/debug/dump", handler.DebugDumpHandler(repo))

	srv := &http.Server{Addr: ":8080", Handler: mux}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	done := make(chan struct{}) // Канал для ожидания graceful shutdown

	go func() {
		<-sigCh
		log.Println("signal received: initiating graceful shutdown")

		// Перестаем принимать новые задачи и останавливаем HTTP-сервер
		svc.StopAccepting()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			log.Printf("http-server shutdown error: %v", err)
		}

		svc.Shutdown() // Дожидаемся завершения текущих задач

		close(done)
	}()

	log.Println("http-server starting on :8080")
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("listen: %v", err)
	}

	<-done // Ждем завершения
}
