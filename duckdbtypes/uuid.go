package duckdbtypes

import (
	"encoding/hex"
	"fmt"

	"github.com/marcboeker/go-duckdb/internal/uuidx"
)

type UUID [uuidx.ByteLength]byte

func (t *UUID) AssignTo(dst any) error {
	switch v := dst.(type) {
	case *[16]byte:
		*v = *t
		return nil
	case *[]byte:
		*v = make([]byte, 16)
		copy(*v, t[:])
		return nil
	case *string:
		*v = t.String()
		return nil
	default:
		if nextDst, retry := GetAssignToDstType(v); retry {
			return t.AssignTo(nextDst)
		}
	}

	return fmt.Errorf("cannot assign %v into %T", t, dst)
}

func (u *UUID) Scan(v any) error {
	switch val := v.(type) {
	case []byte:
		if len(val) != uuidx.ByteLength {
			return u.Scan(string(val))
		}
		copy(u[:], val[:])
		return nil
	case string:
		id, err := uuidx.Parse(val)
		if err != nil {
			return err
		}
		copy(u[:], id[:])
		return nil
	default:
		return fmt.Errorf("invalid UUID value type: %T", val)
	}
}

func (u *UUID) String() string {
	buf := make([]byte, 36)

	hex.Encode(buf, u[:4])
	buf[8] = '-'
	hex.Encode(buf[9:13], u[4:6])
	buf[13] = '-'
	hex.Encode(buf[14:18], u[6:8])
	buf[18] = '-'
	hex.Encode(buf[19:23], u[8:10])
	buf[23] = '-'
	hex.Encode(buf[24:], u[10:])

	return string(buf)
}
