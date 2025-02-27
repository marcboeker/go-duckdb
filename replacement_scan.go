package duckdb

/*
void replacement_scan_callback(void *, void *, void *);
typedef void (*replacement_scan_callback_t)(void *, void *, void *);

void replacement_scan_delete_callback(void *);
typedef void (*replacement_scan_delete_callback_t)(void *);
*/
import "C"

import (
	"runtime/cgo"
	"unsafe"

	m "github.com/marcboeker/go-duckdb/mapping"
)

type ReplacementScanCallback func(tableName string) (string, []any, error)

func RegisterReplacementScan(c *Connector, callback ReplacementScanCallback) {
	h := cgo.NewHandle(callback)
	callbackPtr := unsafe.Pointer(C.replacement_scan_callback_t(C.replacement_scan_callback))
	deleteCallbackPtr := unsafe.Pointer(C.replacement_scan_delete_callback_t(C.replacement_scan_delete_callback))
	m.AddReplacementScan(c.db, callbackPtr, unsafe.Pointer(&h), deleteCallbackPtr)
}

//export replacement_scan_delete_callback
func replacement_scan_delete_callback(info unsafe.Pointer) {
	h := *(*cgo.Handle)(info)
	// FIXME: Should this go through the unpinner?
	h.Delete()
}

//export replacement_scan_callback
func replacement_scan_callback(infoPtr unsafe.Pointer, tableNamePtr unsafe.Pointer, data unsafe.Pointer) {
	info := m.ReplacementScanInfo{Ptr: infoPtr}
	tableName := C.GoString((*C.char)(tableNamePtr))

	h := *(*cgo.Handle)(data)
	scanner := h.Value().(ReplacementScanCallback)
	functionName, params, err := scanner(tableName)
	if err != nil {
		m.ReplacementScanSetError(info, err.Error())
		return
	}
	m.ReplacementScanSetFunctionName(info, functionName)

	for _, param := range params {
		switch paramType := param.(type) {
		case string:
			val := m.CreateVarchar(paramType)
			m.ReplacementScanAddParameter(info, val)
			m.DestroyValue(&val)
		case int64:
			val := m.CreateInt64(paramType)
			m.ReplacementScanAddParameter(info, val)
			m.DestroyValue(&val)
		default:
			m.ReplacementScanSetError(info, "unsupported type for replacement scan")
			return
		}
	}
}
