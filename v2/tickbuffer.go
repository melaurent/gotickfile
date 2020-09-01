package gotickfile

import (
	"bytes"
	"fmt"
	"reflect"
	"unsafe"
)

type TickBuffer struct {
	dataType reflect.Type
	buff     *bytes.Buffer
	tmpVal   reflect.Value
}

func NewTickBuffer(dataType reflect.Type, buf []byte) *TickBuffer {
	buffer := bytes.NewBuffer(buf)
	tb := &TickBuffer{
		dataType: dataType,
		buff:     buffer,
	}

	tb.tmpVal = reflect.New(tb.dataType)

	return tb
}

func (b *TickBuffer) ToSlice() interface{} {
	buff := b.buff.Bytes()
	ptr := unsafe.Pointer(&buff[0])
	itemCount := len(b.buff.Bytes()) / int(b.dataType.Size())
	sliceHeader := &reflect.SliceHeader{
		Data: uintptr(ptr),
		Len:  itemCount,
		Cap:  itemCount,
	}
	sliceHeaderPtr := unsafe.Pointer(sliceHeader)

	sliceTyp := reflect.SliceOf(b.dataType)
	slice := reflect.NewAt(sliceTyp, sliceHeaderPtr)

	return slice.Interface()
}

func (b *TickBuffer) Bytes() []byte {
	return b.buff.Bytes()
}

func (b *TickBuffer) Read(idx int) interface{} {
	ptrIdx := int64(idx) * int64(b.dataType.Size())
	ptr := unsafe.Pointer(&b.buff.Bytes()[ptrIdx])
	val := reflect.NewAt(b.dataType, ptr)
	return val.Interface()
}

func (b *TickBuffer) Write(val interface{}) (int, error) {
	expectedType := reflect.PtrTo(reflect.SliceOf(b.dataType))

	if reflect.TypeOf(val) != expectedType {
		return 0, fmt.Errorf("was expecting pointer to slice of %s, got %s", b.dataType, reflect.TypeOf(val))
	}

	hdr := (*reflect.SliceHeader)(unsafe.Pointer(reflect.ValueOf(val).Pointer()))
	size := hdr.Len * int(b.dataType.Size())

	var sl = struct {
		addr uintptr
		len  int
		cap  int
	}{hdr.Data, size, size}

	p := *(*[]byte)(unsafe.Pointer(&sl))

	return b.buff.Write(p)
}

func (b *TickBuffer) ItemCount() int {
	return len(b.buff.Bytes()) / int(b.dataType.Size())
}
