package duckdb

import (
	"context"
	"sync"
)

// contextStore is an interface for managing connection contexts.
type contextStore interface {
	context(connId uint64) context.Context
	set(connId uint64, ctx context.Context)
	remove(connId uint64)
}

// ctxStore is a thread-safe implementation of contextStore using a connectionId->context map.
type ctxStore struct {
	mu    sync.Mutex
	store map[uint64]context.Context
}

// newContextStore creates a new instance of ctxStore.
func newContextStore() *ctxStore {
	return &ctxStore{
		store: make(map[uint64]context.Context),
	}
}

func (s *ctxStore) context(connId uint64) context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, ok := s.store[connId]
	if !ok {
		return context.Background()
	}

	return ctx
}

func (s *ctxStore) set(connId uint64, ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.store[connId] = ctx
}

func (s *ctxStore) remove(connId uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.store, connId)
}
