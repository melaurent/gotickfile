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

func NewCTickWriter(bw *compress.BBuffer, info *ItemSection, tick uint64, ptr unsafe.Pointer) *CTickWriter {
	ctw := &CTickWriter{
		tickC:   compress.NewTickCompress(bw, tick),
		structC: NewStructCompress(bw, info, ptr),
	}

	return ctw
}

func CTickWriterFromBlock(bw *compress.BBuffer, info *ItemSection, typ reflect.Type) (*CTickWriter, uint64, error) {
	var lastTick uint64
	br := compress.NewBitReader(bw)
	tickDec, tick, err := compress.NewTickDecompress(br)
	if err != nil {
		return nil, 0, err
	}
	structDec, _, err := NewStructDecompress(br, info, typ)
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

func (w *CTickWriter) Write(bw *compress.BBuffer, tick uint64, ptr unsafe.Pointer) {
	w.tickC.Compress(bw, tick)
	w.structC.Compress(bw, ptr)
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

func NewStructCompress(bw *compress.BBuffer, info *ItemSection, ptr unsafe.Pointer) *StructCompress {
	sc := &StructCompress{
		writers: make([]FieldWriter, len(info.Fields)),
	}
	size := info.Info.ItemSize

	for i := len(info.Fields) - 1; i >= 0; i-- {
		f := info.Fields[i]
		fieldPtr := unsafe.Pointer(uintptr(ptr) + uintptr(f.Offset))
		fieldSize := size - f.Offset
		c := compress.GetCompress(bw, fieldPtr, fieldSize, f.CompressionVersion)
		sc.writers[i] = FieldWriter{
			offset: uintptr(f.Offset),
			c:      c,
		}
		size -= fieldSize
	}
	return sc
}

func (c *StructCompress) Compress(bw *compress.BBuffer, ptr unsafe.Pointer) {
	for _, w := range c.writers {
		w.c.Compress(bw, unsafe.Pointer(uintptr(ptr)+w.offset))
	}
}

type StructDecompress struct {
	readers []FieldReader
	val     []byte
	uptr    unsafe.Pointer
	offset  uintptr
	size    uintptr
}

func NewStructDecompress(br *compress.BitReader, info *ItemSection, typ reflect.Type) (*StructDecompress, unsafe.Pointer, error) {
	size := typ.Size()
	sd := &StructDecompress{
		readers: make([]FieldReader, len(info.Fields)),
		val:     make([]byte, size),
		size:    size,
		offset:  0,
	}
	uptr := unsafe.Pointer(&sd.val[0])
	sd.uptr = uptr

	for i := len(info.Fields) - 1; i >= 0; i-- {
		f := info.Fields[i]
		fieldPtr := unsafe.Pointer(uintptr(uptr) + uintptr(f.Offset))
		fieldSize := uint32(size) - f.Offset
		d, err := compress.GetDecompress(br, fieldPtr, fieldSize, f.CompressionVersion)
		if err != nil {
			return nil, nil, fmt.Errorf("error decompressing struct field: %w", err)
		}
		sd.readers[i] = FieldReader{
			offset: uintptr(f.Offset),
			d:      d,
		}
		size -= uintptr(fieldSize)
	}
	sd.offset += typ.Size()

	return sd, uptr, nil
}

func (d *StructDecompress) Decompress(br *compress.BitReader) (unsafe.Pointer, error) {
	if int(d.offset) == cap(d.val) {
		// Need to increase the buffer
		val := make([]byte, 2*d.offset)
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
