package ducktype

import (
	"database/sql/driver"
	"fmt"
	"strconv"
)

// exists because stdlib dont support uint64 values with high bits set.
type NullUint64 struct {
	V     uint64
	Valid bool
}

func (n *NullUint64) Scan(value any) error {
	if value == nil {
		n.V, n.Valid = 0, false
		return nil
	}
	n.Valid = true
	switch v := value.(type) {
	case uint64:
		n.V = v
		return nil
	case int64:
		if v < 0 {
			return fmt.Errorf("nulluint64: cannot scan negative int64 %d into uint64", v)
		}
		n.V = uint64(v)
		return nil
	case float64:
		if v < 0 {
			return fmt.Errorf("nulluint64: cannot scan negative float64 %f into uint64", v)
		}
		n.V = uint64(v)
		return nil
	case []byte:
		parsed, err := strconv.ParseUint(string(v), 10, 64)
		if err != nil {
			return fmt.Errorf("nulluint64: failed to parse []byte %q as uint64: %w", v, err)
		}
		n.V = parsed
		return nil
	case string:
		parsed, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return fmt.Errorf("nulluint64: failed to parse string %q as uint64: %w", v, err)
		}
		n.V = parsed
		return nil
	default:
		n.Valid = false
		return fmt.Errorf("nulluint64: cannot scan type %T into NullUint64", value)
	}
}

func (n NullUint64) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}

	return strconv.FormatUint(n.V, 10), nil
}
