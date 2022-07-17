package gotickfile

import (
	"fmt"
	"github.com/melaurent/gotickfile/v2/compress"
	"io"
	"reflect"
	"unsafe"
)

type CTickWriter struct {
	tickC   *compress.TickCompress
	structC *StructCompress
}

func NewCTickWriter(info *ItemSection, tick uint64, ptr uintptr, bw *compress.BBuffer) *CTickWriter {
	ctw := &CTickWriter{
		tickC:   compress.NewTickCompress(tick, bw),
		structC: NewStructCompress(info, ptr, bw),
	}

	return ctw
}

func CTickWriterFromBlock(info *ItemSection, typ reflect.Type, bw *compress.BBuffer) (*CTickWriter, uint64, error) {
	var lastTick uint64
	br := compress.NewBitReader(bw)
	tickDec, tick, err := compress.NewTickDecompress(br)
	if err != nil {
		return nil, 0, err
	}
	structDec, _, err := NewStructDecompress(info, typ, br)
	if err != nil {
		return nil, 0, err
	}
	lastTick = tick
	for {
		tick, err := tickDec.Decompress(br)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return nil, 0, err
			}
		}
		lastTick = tick
		_, err = structDec.Decompress(br)
		if err != nil {
			return nil, 0, err
		}
	}
	// Now we have a decompressor with the correct state. We have to rewind the last bits used to indicate EOF

	tickC := tickDec.ToCompress()
	structC := structDec.ToCompress()

	return &CTickWriter{
		tickC:   tickC,
		structC: structC,
	}, lastTick, nil
}

func (w *CTickWriter) Write(tick uint64, ptr uintptr, bw *compress.BBuffer) {
	w.tickC.Compress(tick, bw)
	w.structC.Compress(ptr, bw)
}

func (w *CTickWriter) Open(bw *compress.BBuffer) error {
	return w.tickC.Open(bw)
}

func (w *CTickWriter) Close(bw *compress.BBuffer) {
	w.tickC.Close(bw)
}

type FieldWriter struct {
	offset uintptr
	c      compress.Compress
}

type FieldReader struct {
	offset uintptr
	d      compress.Decompress
}

type StructCompress struct {
	writers []FieldWriter
}

func NewStructCompress(info *ItemSection, ptr uintptr, bw *compress.BBuffer) *StructCompress {
	sc := &StructCompress{
		writers: nil,
	}
	for _, f := range info.Fields {
		c := compress.GetCompress(*(*uint64)(unsafe.Pointer(ptr + uintptr(f.Offset))), bw, f.CompressionVersion)
		sc.writers = append(sc.writers, FieldWriter{
			offset: uintptr(f.Offset),
			c:      c,
		})
	}

	return sc
}

func (c *StructCompress) Compress(ptr uintptr, bw *compress.BBuffer) {
	for _, w := range c.writers {
		w.c.Compress(*(*uint64)(unsafe.Pointer(ptr + w.offset)), bw)
	}
}

type StructDecompress struct {
	readers []FieldReader
	val     []byte
	uptr    unsafe.Pointer
	offset  uintptr
	size    uintptr
}

func NewStructDecompress(info *ItemSection, typ reflect.Type, br *compress.BitReader) (*StructDecompress, unsafe.Pointer, error) {
	size := typ.Size()
	sd := &StructDecompress{
		readers: nil,
		val:     make([]byte, size, size),
		size:    size,
		offset:  0,
	}
	uptr := unsafe.Pointer(&sd.val[0])
	sd.uptr = uptr

	ptr := uintptr(uptr)
	for _, f := range info.Fields {
		fptr := ptr + uintptr(f.Offset)
		d, err := compress.GetDecompress(br, unsafe.Pointer(fptr), f.CompressionVersion)
		if err != nil {
			return nil, nil, fmt.Errorf("error decompressing struct field: %v", err)
		}
		sd.readers = append(sd.readers, FieldReader{
			offset: uintptr(f.Offset),
			d:      d,
		})
	}
	sd.offset += size

	return sd, uptr, nil
}

func (d *StructDecompress) Decompress(br *compress.BitReader) (unsafe.Pointer, error) {
	if int(d.offset) == cap(d.val) {
		// Need to increase the buffer
		val := make([]byte, 2*d.offset, 2*d.offset)
		copy(val, d.val)
		d.val = val
		d.uptr = unsafe.Pointer(&val[0])
	}
	for _, r := range d.readers {
		uptr := unsafe.Pointer(uintptr(d.uptr) + d.offset + r.offset)
		if err := r.d.Decompress(br, uptr); err != nil {
			return d.uptr, err
		}
	}
	d.offset += d.size
	return d.uptr, nil
}

func (d *StructDecompress) Clear() {
	d.offset = 0
}

func (d *StructDecompress) ToCompress() *StructCompress {
	sc := &StructCompress{writers: nil}
	for _, r := range d.readers {
		sc.writers = append(sc.writers, FieldWriter{
			offset: r.offset,
			c:      r.d.ToCompress(),
		})
	}
	return sc
}
