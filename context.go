package duckdb

import (
	"context"
	"sync"
)

// ctxConnIdKeyType is a type for the context key used to store connection IDs.
type ctxConnIdKeyType string

// ctxConnIdKey is a key used to store the connection ID in the context.
var ctxConnIdKey ctxConnIdKeyType = "duckdb_conn_id"

// contextStore is an interface for managing connection contexts.
type contextStore interface {
	Context(connId uint64) context.Context
	SetContext(connId uint64, ctx context.Context)
	Remove(connId uint64)
}

// ctxStore is a thread-safe implementation of contextStore that uses a map to store contexts by connection ID.
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

// Context retrieves the context associated with the given connection ID.
func (s *ctxStore) Context(connId uint64) context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, ok := s.store[connId]
	if !ok {
		return context.Background()
	}
	return context.WithValue(ctx, ctxConnIdKey, connId)
}

// SetContext sets the context for a given connection ID.
func (s *ctxStore) SetContext(connId uint64, ctx context.Context) {
	s.store[connId] = ctx
}

// Remove deletes the context associated with the given connection ID.
func (s *ctxStore) Remove(connId uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.store, connId)
}

// ConnectionId retrieves the connection ID from the context, if it exists.
func ConnectionId(ctx context.Context) (uint64, bool) {
	if ctx == nil {
		return 0, false
	}
	connId, ok := ctx.Value(ctxConnIdKey).(uint64)
	return connId, ok
}
