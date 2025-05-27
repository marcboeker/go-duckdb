package duckdb

import (
	"context"
	"sync"
)

// contextStore stores the thread-safe context of a connection.
type contextStore struct {
	m sync.Map
}

// newContextStore creates a new instance of ctxStore.
func newContextStore() *contextStore {
	return &contextStore{
		m: sync.Map{},
	}
}

func (s *contextStore) load(connId uint64) context.Context {
	v, ok := s.m.Load(connId)
	if !ok {
		return context.Background()
	}
	ctx, ok := v.(context.Context)
	if !ok {
		return context.Background()
	}

	return ctx
}

func (s *contextStore) store(connId uint64, ctx context.Context) func() {
	s.m.Store(connId, ctx)

	return func() {
		s.m.Delete(connId)
	}
}

func (s *contextStore) delete(connId uint64) {
	s.m.Delete(connId)
}
