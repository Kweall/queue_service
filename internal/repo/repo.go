package repo

import "sync"

// Тело JSON: {"id":"<string>","payload":"<string>","max_retries":<int>}
type Task struct {
	ID         string `json:"id"`
	Payload    string `json:"payload"`
	MaxRetries int    `json:"max_retries"`
}

type TaskMeta struct {
	Task     Task
	Attempts int
	State    string
}

type Repo struct {
	mu    sync.RWMutex
	store map[string]*TaskMeta
}

func NewRepo() *Repo {
	return &Repo{store: make(map[string]*TaskMeta)}
}

func (r *Repo) CreateOrUpdate(task Task, state string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	m, ok := r.store[task.ID]
	if !ok {
		m = &TaskMeta{Task: task, Attempts: 0, State: state}
		r.store[task.ID] = m
		return
	}
	m.Task = task
	m.State = state
	m.Attempts = 0
}

func (r *Repo) SetState(id string, state string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if m, ok := r.store[id]; ok {
		m.State = state
	}
}

func (r *Repo) IncAttempts(id string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	if m, ok := r.store[id]; ok {
		m.Attempts++
		return m.Attempts
	}
	return 0
}

func (r *Repo) Get(id string) (TaskMeta, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if m, ok := r.store[id]; ok {
		return *m, true
	}
	return TaskMeta{}, false
}

func (r *Repo) DumpAll() map[string]TaskMeta {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]TaskMeta, len(r.store))
	for k, v := range r.store {
		out[k] = *v
	}
	return out
}
