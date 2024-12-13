package duckdbtypes

import "github.com/go-viper/mapstructure/v2"

func NewComposite[T any](v T) Composite[T] {
	return Composite[T]{v}
}

// Use as the `Scanner` type for any composite types (maps, lists, structs)
type Composite[T any] struct {
	t T
}

func (s Composite[T]) Get() T {
	return s.t
}

func (s *Composite[T]) Scan(v any) error {
	return mapstructure.Decode(v, &s.t)
}
