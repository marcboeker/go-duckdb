package duckdb

import (
	"context"
	"sync"
)

type ctxConnIdKeyType string

var ctxConnIdKey ctxConnIdKeyType = "duckdb_conn_id"

type contextStore interface {
	Context(connId uint64) context.Context
	SetContext(connId uint64, ctx context.Context)
	Remove(connId uint64) error
}

type ctxStore struct {
	mu    sync.Mutex
	store map[uint64]context.Context
}

func newContextStore() *ctxStore {
	return &ctxStore{
		store: make(map[uint64]context.Context),
	}
}

func (s *ctxStore) Context(connId uint64) context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, ok := s.store[connId]
	if !ok {
		return context.Background()
	}
	return context.WithValue(ctx, ctxConnIdKey, connId)
}

func (s *ctxStore) SetContext(connId uint64, ctx context.Context) {
	s.store[connId] = ctx
}

func (s *ctxStore) Remove(connId uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.store, connId)

	return nil
}

func ConnectionId(ctx context.Context) (uint64, bool) {
	if ctx == nil {
		return 0, false
	}
	connId, ok := ctx.Value(ctxConnIdKey).(uint64)
	return connId, ok
}
