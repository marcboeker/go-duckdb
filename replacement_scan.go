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
)

type ReplacementScanCallback func(tableName string) (string, []any, error)

func RegisterReplacementScan(c *Connector, callback ReplacementScanCallback) {
	h := cgo.NewHandle(callback)
	callbackPtr := unsafe.Pointer(C.replacement_scan_callback_t(C.replacement_scan_callback))
	deleteCallbackPtr := unsafe.Pointer(C.replacement_scan_delete_callback_t(C.replacement_scan_delete_callback))
	apiAddReplacementScan(c.db, callbackPtr, unsafe.Pointer(&h), deleteCallbackPtr)
}

//export replacement_scan_delete_callback
func replacement_scan_delete_callback(info unsafe.Pointer) {
	h := *(*cgo.Handle)(info)
	// FIXME: Should this go through the unpinner?
	h.Delete()
}

//export replacement_scan_callback
func replacement_scan_callback(infoPtr unsafe.Pointer, tableNamePtr unsafe.Pointer, data unsafe.Pointer) {
	info := apiReplacementScanInfo{Ptr: infoPtr}
	tableName := C.GoString((*C.char)(tableNamePtr))

	h := *(*cgo.Handle)(data)
	scanner := h.Value().(ReplacementScanCallback)
	functionName, params, err := scanner(tableName)
	if err != nil {
		apiReplacementScanSetError(info, err.Error())
		return
	}
	apiReplacementScanSetFunctionName(info, functionName)

	for _, param := range params {
		switch paramType := param.(type) {
		case string:
			val := apiCreateVarchar(paramType)
			apiReplacementScanAddParameter(info, val)
			apiDestroyValue(&val)
		case int64:
			val := apiCreateInt64(paramType)
			apiReplacementScanAddParameter(info, val)
			apiDestroyValue(&val)
		default:
			apiReplacementScanSetError(info, "unsupported type for replacement scan")
			return
		}
	}
}
