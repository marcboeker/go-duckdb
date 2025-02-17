package duckdb

import (
	"reflect"
	"unsafe"
)

// vector storage of a DuckDB column.
type vector struct {
	// The underlying DuckDB vector.
	apiVec apiVector
	// The underlying data ptr.
	dataPtr unsafe.Pointer
	// The vector's validity mask.
	maskPtr unsafe.Pointer
	// A callback function to get a value from this vector.
	getFn fnGetVectorValue
	// A callback function to write to this vector.
	setFn fnSetVectorValue
	// The child vectors of nested data types.
	childVectors []vector

	// The vector's type information.
	vectorTypeInfo
}

func (*vector) canNil(val reflect.Value) bool {
	switch val.Kind() {
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Pointer,
		reflect.UnsafePointer, reflect.Interface, reflect.Slice:
		return true
	default:
		return false
	}
}

func (vec *vector) init(logicalType apiLogicalType, colIdx int) error {
	t := Type(apiGetTypeId(logicalType))
	name, inMap := unsupportedTypeToStringMap[t]
	if inMap {
		return addIndexToError(unsupportedTypeError(name), colIdx)
	}

	alias := apiLogicalTypeGetAlias(logicalType)
	switch alias {
	case aliasJSON:
		vec.initJSON()
		return nil
	}

	switch t {
	case TYPE_BOOLEAN:
		initBool(vec)
	case TYPE_TINYINT:
		initNumeric[int8](vec, t)
	case TYPE_SMALLINT:
		initNumeric[int16](vec, t)
	case TYPE_INTEGER:
		initNumeric[int32](vec, t)
	case TYPE_BIGINT:
		initNumeric[int64](vec, t)
	case TYPE_UTINYINT:
		initNumeric[uint8](vec, t)
	case TYPE_USMALLINT:
		initNumeric[uint16](vec, t)
	case TYPE_UINTEGER:
		initNumeric[uint32](vec, t)
	case TYPE_UBIGINT:
		initNumeric[uint64](vec, t)
	case TYPE_FLOAT:
		initNumeric[float32](vec, t)
	case TYPE_DOUBLE:
		initNumeric[float64](vec, t)
	case TYPE_TIMESTAMP, TYPE_TIMESTAMP_S, TYPE_TIMESTAMP_MS, TYPE_TIMESTAMP_NS, TYPE_TIMESTAMP_TZ:
		vec.initTS(t)
	case TYPE_DATE:
		vec.initDate()
	case TYPE_TIME, TYPE_TIME_TZ:
		vec.initTime(t)
	case TYPE_INTERVAL:
		vec.initInterval()
	case TYPE_HUGEINT:
		vec.initHugeint()
	case TYPE_VARCHAR, TYPE_BLOB:
		vec.initBytes(t)
	case TYPE_DECIMAL:
		return vec.initDecimal(logicalType, colIdx)
	case TYPE_ENUM:
		return vec.initEnum(logicalType, colIdx)
	case TYPE_LIST:
		return vec.initList(logicalType, colIdx)
	case TYPE_STRUCT:
		return vec.initStruct(logicalType, colIdx)
	case TYPE_MAP:
		return vec.initMap(logicalType, colIdx)
	case TYPE_ARRAY:
		return vec.initArray(logicalType, colIdx)
	case TYPE_UUID:
		vec.initUUID()
	case TYPE_SQLNULL:
		vec.initSQLNull()
	default:
		return addIndexToError(unsupportedTypeError(unknownTypeErrMsg), colIdx)
	}
	return nil
}

func (vec *vector) resizeListVector(newLength uint64) {
	apiListVectorReserve(vec.apiVec, newLength)
	apiListVectorSetSize(vec.apiVec, newLength)
	vec.resetChildData()
}

func (vec *vector) resetChildData() {
	for i := range vec.childVectors {
		vec.childVectors[i].dataPtr = apiVectorGetData(vec.childVectors[i].apiVec)
		vec.childVectors[i].resetChildData()
	}
}

func (vec *vector) initVectors(apiVec apiVector, writable bool) {
	vec.apiVec = apiVec
	vec.dataPtr = apiVectorGetData(apiVec)
	if writable {
		apiVectorEnsureValidityWritable(apiVec)
	}
	vec.maskPtr = apiVectorGetValidity(apiVec)
	vec.getChildVectors(apiVec, writable)
}

func (vec *vector) getChildVectors(apiVec apiVector, writable bool) {
	switch vec.Type {
	case TYPE_LIST, TYPE_MAP:
		child := apiListVectorGetChild(apiVec)
		vec.childVectors[0].initVectors(child, writable)
	case TYPE_STRUCT:
		for i := 0; i < len(vec.childVectors); i++ {
			child := apiStructVectorGetChild(apiVec, uint64(i))
			vec.childVectors[i].initVectors(child, writable)
		}
	case TYPE_ARRAY:
		child := apiArrayVectorGetChild(apiVec)
		vec.childVectors[0].initVectors(child, writable)
	}
}

func initBool(vec *vector) {
	vec.getFn = func(vec *vector, rowIdx uint64) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		return getPrimitive[bool](vec, rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx uint64, val any) error {
		if val == nil {
			vec.setNull(rowIdx)
			return nil
		}
		return setBool(vec, rowIdx, val)
	}
	vec.Type = TYPE_BOOLEAN
}

func initNumeric[T numericType](vec *vector, t Type) {
	vec.getFn = func(vec *vector, rowIdx uint64) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		return getPrimitive[T](vec, rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx uint64, val any) error {
		if val == nil {
			vec.setNull(rowIdx)
			return nil
		}
		return setNumeric[any, T](vec, rowIdx, val)
	}
	vec.Type = t
}

func (vec *vector) initTS(t Type) {
	vec.getFn = func(vec *vector, rowIdx uint64) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		return vec.getTS(t, rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx uint64, val any) error {
		if val == nil {
			vec.setNull(rowIdx)
			return nil
		}
		return setTS(vec, rowIdx, val)
	}
	vec.Type = t
}

func (vec *vector) initDate() {
	vec.getFn = func(vec *vector, rowIdx uint64) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		return vec.getDate(rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx uint64, val any) error {
		if val == nil {
			vec.setNull(rowIdx)
			return nil
		}
		return setDate(vec, rowIdx, val)
	}
	vec.Type = TYPE_DATE
}

func (vec *vector) initTime(t Type) {
	vec.getFn = func(vec *vector, rowIdx uint64) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		return vec.getTime(rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx uint64, val any) error {
		if val == nil {
			vec.setNull(rowIdx)
			return nil
		}
		return setTime(vec, rowIdx, val)
	}
	vec.Type = t
}

func (vec *vector) initInterval() {
	vec.getFn = func(vec *vector, rowIdx uint64) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		return vec.getInterval(rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx uint64, val any) error {
		if val == nil {
			vec.setNull(rowIdx)
			return nil
		}
		return setInterval(vec, rowIdx, val)
	}
	vec.Type = TYPE_INTERVAL
}

func (vec *vector) initHugeint() {
	vec.getFn = func(vec *vector, rowIdx uint64) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		return vec.getHugeint(rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx uint64, val any) error {
		if val == nil {
			vec.setNull(rowIdx)
			return nil
		}
		return setHugeint(vec, rowIdx, val)
	}
	vec.Type = TYPE_HUGEINT
}

func (vec *vector) initBytes(t Type) {
	vec.getFn = func(vec *vector, rowIdx uint64) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		return vec.getBytes(rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx uint64, val any) error {
		if val == nil {
			vec.setNull(rowIdx)
			return nil
		}
		return setBytes(vec, rowIdx, val)
	}
	vec.Type = t
}

func (vec *vector) initJSON() {
	vec.getFn = func(vec *vector, rowIdx uint64) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		return vec.getJSON(rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx uint64, val any) error {
		if val == nil {
			vec.setNull(rowIdx)
			return nil
		}
		return setJSON(vec, rowIdx, val)
	}
	vec.Type = TYPE_VARCHAR
}

func (vec *vector) initDecimal(logicalType apiLogicalType, colIdx int) error {
	vec.decimalWidth = apiDecimalWidth(logicalType)
	vec.decimalScale = apiDecimalScale(logicalType)

	t := Type(apiDecimalInternalType(logicalType))
	switch t {
	case TYPE_SMALLINT, TYPE_INTEGER, TYPE_BIGINT, TYPE_HUGEINT:
		vec.getFn = func(vec *vector, rowIdx uint64) any {
			if vec.getNull(rowIdx) {
				return nil
			}
			return vec.getDecimal(rowIdx)
		}
		vec.setFn = func(vec *vector, rowIdx uint64, val any) error {
			if val == nil {
				vec.setNull(rowIdx)
				return nil
			}
			return setDecimal(vec, rowIdx, val)
		}
	default:
		return addIndexToError(unsupportedTypeError(typeToStringMap[t]), colIdx)
	}

	vec.Type = TYPE_DECIMAL
	vec.internalType = t
	return nil
}

func (vec *vector) initEnum(logicalType apiLogicalType, colIdx int) error {
	// Initialize the dictionary.
	dictSize := apiEnumDictionarySize(logicalType)
	vec.dict = make(map[string]uint32)

	for i := uint32(0); i < dictSize; i++ {
		str := apiEnumDictionaryValue(logicalType, uint64(i))
		vec.dict[str] = i
	}

	t := Type(apiEnumInternalType(logicalType))
	switch t {
	case TYPE_UTINYINT, TYPE_USMALLINT, TYPE_UINTEGER, TYPE_UBIGINT:
		vec.getFn = func(vec *vector, rowIdx uint64) any {
			if vec.getNull(rowIdx) {
				return nil
			}
			return vec.getEnum(rowIdx)
		}
		vec.setFn = func(vec *vector, rowIdx uint64, val any) error {
			if val == nil {
				vec.setNull(rowIdx)
				return nil
			}
			return setEnum(vec, rowIdx, val)
		}
	default:
		return addIndexToError(unsupportedTypeError(typeToStringMap[t]), colIdx)
	}

	vec.Type = TYPE_ENUM
	vec.internalType = t
	return nil
}

func (vec *vector) initList(logicalType apiLogicalType, colIdx int) error {
	// Get the child vector type.
	childType := apiListTypeChildType(logicalType)
	defer apiDestroyLogicalType(&childType)

	// Recurse into the child.
	vec.childVectors = make([]vector, 1)
	err := vec.childVectors[0].init(childType, colIdx)
	if err != nil {
		return err
	}

	vec.getFn = func(vec *vector, rowIdx uint64) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		return vec.getList(rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx uint64, val any) error {
		if val == nil {
			vec.setNull(rowIdx)
			return nil
		}
		return setList(vec, rowIdx, val)
	}
	vec.Type = TYPE_LIST
	return nil
}

func (vec *vector) initStruct(logicalType apiLogicalType, colIdx int) error {
	childCount := int(apiStructTypeChildCount(logicalType))
	var structEntries []StructEntry
	for i := 0; i < childCount; i++ {
		name := apiStructTypeChildName(logicalType, uint64(i))
		entry, err := NewStructEntry(nil, name)
		structEntries = append(structEntries, entry)
		if err != nil {
			return err
		}
	}

	vec.childVectors = make([]vector, childCount)
	vec.structEntries = structEntries

	// Recurse into the children.
	for i := 0; i < childCount; i++ {
		childType := apiStructTypeChildType(logicalType, uint64(i))
		err := vec.childVectors[i].init(childType, colIdx)
		apiDestroyLogicalType(&childType)

		if err != nil {
			return err
		}
	}

	vec.getFn = func(vec *vector, rowIdx uint64) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		return vec.getStruct(rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx uint64, val any) error {
		if val == nil {
			vec.setNull(rowIdx)
			return nil
		}
		return setStruct(vec, rowIdx, val)
	}
	vec.Type = TYPE_STRUCT
	return nil
}

func (vec *vector) initMap(logicalType apiLogicalType, colIdx int) error {
	// A MAP is a LIST of STRUCT values. Each STRUCT holds two children: a key and a value.

	// Get the child vector type.
	childType := apiListTypeChildType(logicalType)
	defer apiDestroyLogicalType(&childType)

	// Recurse into the child.
	vec.childVectors = make([]vector, 1)
	err := vec.childVectors[0].init(childType, colIdx)
	if err != nil {
		return err
	}

	// DuckDB supports more MAP key types than Go, which only supports comparable types.
	// We ensure that the key type itself is comparable.
	keyType := apiMapTypeKeyType(logicalType)
	defer apiDestroyLogicalType(&keyType)

	t := Type(apiGetTypeId(keyType))
	switch t {
	case TYPE_LIST, TYPE_STRUCT, TYPE_MAP, TYPE_ARRAY:
		return addIndexToError(errUnsupportedMapKeyType, colIdx)
	}

	vec.getFn = func(vec *vector, rowIdx uint64) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		return vec.getMap(rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx uint64, val any) error {
		if val == nil {
			vec.setNull(rowIdx)
			return nil
		}
		return setMap(vec, rowIdx, val)
	}
	vec.Type = TYPE_MAP
	return nil
}

func (vec *vector) initArray(logicalType apiLogicalType, colIdx int) error {
	vec.arrayLength = apiArrayTypeArraySize(logicalType)

	// Get the child vector type.
	childType := apiArrayTypeChildType(logicalType)
	defer apiDestroyLogicalType(&childType)

	// Recurse into the child.
	vec.childVectors = make([]vector, 1)
	err := vec.childVectors[0].init(childType, colIdx)
	if err != nil {
		return err
	}

	vec.getFn = func(vec *vector, rowIdx uint64) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		return vec.getArray(rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx uint64, val any) error {
		if val == nil {
			vec.setNull(rowIdx)
			return nil
		}
		return setArray(vec, rowIdx, val)
	}
	vec.Type = TYPE_ARRAY
	return nil
}

func (vec *vector) initUUID() {
	vec.getFn = func(vec *vector, rowIdx uint64) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		hugeInt := getPrimitive[apiHugeInt](vec, rowIdx)
		return hugeIntToUUID(hugeInt)
	}
	vec.setFn = func(vec *vector, rowIdx uint64, val any) error {
		if val == nil || val == (*UUID)(nil) {
			vec.setNull(rowIdx)
			return nil
		}
		return setUUID(vec, rowIdx, val)
	}
	vec.Type = TYPE_UUID
}

func (vec *vector) initSQLNull() {
	vec.getFn = func(vec *vector, rowIdx uint64) any {
		return nil
	}
	vec.setFn = func(vec *vector, rowIdx uint64, val any) error {
		return errSetSQLNULLValue
	}
	vec.Type = TYPE_SQLNULL
}
