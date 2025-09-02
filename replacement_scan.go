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

	"github.com/marcboeker/go-duckdb/mapping"
)

type ReplacementScanCallback func(tableName string) (string, []any, error)

func RegisterReplacementScan(c *Connector, callback ReplacementScanCallback) {
	h := cgo.NewHandle(callback)
	callbackPtr := unsafe.Pointer(C.replacement_scan_callback_t(C.replacement_scan_callback))
	deleteCallbackPtr := unsafe.Pointer(C.replacement_scan_delete_callback_t(C.replacement_scan_delete_callback))
	mapping.AddReplacementScan(c.db, callbackPtr, unsafe.Pointer(&h), deleteCallbackPtr)
}

//export replacement_scan_delete_callback
func replacement_scan_delete_callback(info unsafe.Pointer) {
	h := *(*cgo.Handle)(info)
	// FIXME: Should this go through the unpinner?
	h.Delete()
}

//export replacement_scan_callback
func replacement_scan_callback(infoPtr, tableNamePtr, data unsafe.Pointer) {
	info := mapping.ReplacementScanInfo{Ptr: infoPtr}
	tableName := C.GoString((*C.char)(tableNamePtr))

	h := *(*cgo.Handle)(data)
	scanner := h.Value().(ReplacementScanCallback)
	functionName, params, err := scanner(tableName)
	if err != nil {
		mapping.ReplacementScanSetError(info, err.Error())
		return
	}
	mapping.ReplacementScanSetFunctionName(info, functionName)

	for _, param := range params {
		switch paramType := param.(type) {
		case string:
			val := mapping.CreateVarchar(paramType)
			mapping.ReplacementScanAddParameter(info, val)
			mapping.DestroyValue(&val)
		case int64:
			val := mapping.CreateInt64(paramType)
			mapping.ReplacementScanAddParameter(info, val)
			mapping.DestroyValue(&val)
		default:
			mapping.ReplacementScanSetError(info, "unsupported type for replacement scan")
			return
		}
	}
}
