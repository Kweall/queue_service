package service

import (
	"context"
	"errors"
	"log"
	"math"
	"math/rand"
	"queue-service/internal/repo"
	"sync"
	"time"
)

type Service struct {
	repo      *repo.Repo
	queue     chan repo.Task
	workers   int
	ctx       context.Context
	cancel    context.CancelFunc
	acceptMtx sync.RWMutex
	accepting bool
	wg        sync.WaitGroup // воркеры
	active    sync.WaitGroup // активные задачи
}

func NewService(r *repo.Repo, workers, queueSize int) *Service {
	ctx, cancel := context.WithCancel(context.Background())
	return &Service{
		repo:      r,
		queue:     make(chan repo.Task, queueSize),
		workers:   workers,
		ctx:       ctx,
		cancel:    cancel,
		accepting: true,
	}
}

func (s *Service) Start() {
	log.Printf("starting service: workers=%d, queue_size=%d", s.workers, cap(s.queue))
	for i := 0; i < s.workers; i++ {
		s.wg.Add(1)
		go s.worker(i)
	}
}

func (s *Service) StopAccepting() {
	s.acceptMtx.Lock()
	defer s.acceptMtx.Unlock()
	s.accepting = false
	log.Println("service: stopped accepting new tasks")
}

func (s *Service) IsAccepting() bool {
	s.acceptMtx.RLock()
	defer s.acceptMtx.RUnlock()
	return s.accepting
}

// Корректно завершаем программу
func (s *Service) Shutdown() {
	log.Println("shutdown initiated")
	// Перестаём принимать новые задачи
	s.StopAccepting()

	// Дожидаемся завершения текущих
	log.Println("shutdown: waiting for active tasks to finish")
	s.active.Wait()

	log.Println("shutdown: cancelling background context")
	s.cancel()

	// Дожидаемся всех воркеров
	s.wg.Wait()

	log.Println("shutdown complete")
}

func (s *Service) Enqueue(t repo.Task) error {
	if !s.IsAccepting() {
		return errors.New("service not accepting new tasks")
	}
	s.repo.CreateOrUpdate(t, "queued")
	log.Printf("[enqueue] id=%s queued", t.ID)

	select {
	case s.queue <- t:
		return nil
	default:
		return errors.New("queue is full")
	}
}

func (s *Service) worker(id int) {
	defer s.wg.Done()
	log.Printf("worker-%d started", id)
	for {
		select {
		case <-s.ctx.Done():
			log.Printf("worker-%d exiting", id)
			return
		case t := <-s.queue:
			s.processTask(id, t)
		}
	}
}

func (s *Service) processTask(workerID int, t repo.Task) {
	s.active.Add(1)
	defer s.active.Done()

	s.repo.SetState(t.ID, "running")
	log.Printf("[worker-%d] started task:%s", workerID, t.ID)

	work := time.Duration(10+rand.Intn(10)) * time.Second
	// work := time.Duration(100+rand.Intn(401)) * time.Millisecond // Каждое задание «работает» 100–500 мс (симулируем обработку)
	time.Sleep(work)

	fail := rand.Intn(100) < 50 // 20% задач «падают» (симулируем ошибки)
	if !fail {
		s.repo.SetState(t.ID, "done")
		log.Printf("[worker-%d] finished task:%s (done)", workerID, t.ID)
		return
	}

	// Если произошла ошибка, инкрементируем attempts (для проверки max_retries)
	attempts := s.repo.IncAttempts(t.ID)
	log.Printf("[worker-%d] task:%s failed (attempt %d)", workerID, t.ID, attempts)

	// Если попытки еще есть, ставим в очередь через scheduleRetry
	if attempts <= t.MaxRetries {
		backoff := s.scheduleRetry(t, attempts)
		s.repo.SetState(t.ID, "queued")
		log.Printf("[worker-%d] retry scheduled for task:%s after %v (attempt %d)", workerID, t.ID, backoff, attempts)
		return
	}

	// Если max_retries превышено
	s.repo.SetState(t.ID, "failed")
	log.Printf("[worker-%d] permanently failed task:%s after %d attempts", workerID, t.ID, attempts)
}

func (s *Service) scheduleRetry(t repo.Task, attempt int) time.Duration {
	// экспоненциальный бэкофф с джиттером
	base := 100 * time.Millisecond
	// pow = 2^(attempt-1)
	pow := time.Duration(1 << uint(attempt-1))
	backoff := time.Duration(int64(base) * int64(pow))
	// jitter +-50 процентов
	jitter := time.Duration(rand.Int63n(int64(backoff/2) + 1))
	if rand.Intn(2) == 0 {
		backoff = backoff - jitter
	} else {
		backoff = backoff + jitter
	}
	if backoff < 0 || backoff > time.Duration(math.MaxInt64/2) {
		backoff = base
	}

	// Горутина ждет и пытается заново поставить задачу в очередь, если сервис всё ещё принимает задачи и контекст не отменен
	go func() {
		select {
		case <-time.After(backoff):
			// Проверка shutdown
			if !s.IsAccepting() || s.ctx.Err() != nil {
				log.Printf("[task:%s] not re-enqueuing because service shutting down; marking failed", t.ID)
				s.repo.SetState(t.ID, "failed")
				return
			}
			// Попытка поставить в очередь, если она заполнена
			select {
			case s.queue <- t:
				log.Printf("[task:%s] re-enqueued (attempt %d)", t.ID, attempt)
			case <-s.ctx.Done():
				log.Printf("[task:%s] retry canceled due to shutdown", t.ID)
				s.repo.SetState(t.ID, "failed")
			}
		case <-s.ctx.Done():
			log.Printf("[task:%s] retry canceled before delay finished", t.ID)
			s.repo.SetState(t.ID, "failed")
		}
	}()

	return backoff
}
