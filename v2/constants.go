package gotickfile

import (
	uuid "github.com/satori/go.uuid"
	"reflect"
)

const (
	ITEM_SECTION_ID                int32 = 0x0a
	CONTENT_DESCRIPTION_SECTION_ID int32 = 0x80
	NAME_VALUE_SECTION_ID          int32 = 0x81
	TAGS_SECTION_ID                int32 = 0x82

	NAME_VALUE_INT32  int32 = 3
	NAME_VALUE_UINT64 int32 = 5
	NAME_VALUE_DOUBLE int32 = 10
	NAME_VALUE_BYTES  int32 = 11
	NAME_VALUE_TEXT   int32 = 12
	NAME_VALUE_UUID   int32 = 13

	INT8    uint8 = 1
	INT16   uint8 = 2
	INT32   uint8 = 3
	INT64   uint8 = 4
	UINT8   uint8 = 5
	UINT16  uint8 = 6
	UINT32  uint8 = 7
	UINT64  uint8 = 8
	FLOAT32 uint8 = 9
	FLOAT64 uint8 = 10
	ARRAY   uint8 = 11
)

var fieldTypeToKind = map[uint8]reflect.Kind{
	INT8:    reflect.Int8,
	INT16:   reflect.Int16,
	INT32:   reflect.Int32,
	INT64:   reflect.Int64,
	UINT8:   reflect.Uint8,
	UINT16:  reflect.Uint16,
	UINT32:  reflect.Uint32,
	UINT64:  reflect.Uint64,
	FLOAT32: reflect.Float32,
	FLOAT64: reflect.Float64,
	ARRAY:   reflect.Array,
}

var kindToFieldType = make(map[reflect.Kind]uint8)

var typeToNameValueType = map[string]int32{
	reflect.TypeOf(int32(0)).String():    3,
	reflect.TypeOf(uint64(0)).String():   5,
	reflect.TypeOf(float64(0)).String():  10,
	reflect.TypeOf([]byte{}).String():    11,
	reflect.TypeOf("").String():          12,
	reflect.TypeOf(uuid.UUID{}).String(): 13,
}

func init() {
	for field, kind := range fieldTypeToKind {
		kindToFieldType[kind] = field
	}
}
