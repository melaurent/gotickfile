package gotickfile

import (
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

func CTickWriterFromBlock(info *ItemSection, typ reflect.Type, bw *compress.BBuffer) (*CTickWriter, error) {
	br := compress.NewBReader(bw)
	tickDec, _, err := compress.NewTickDecompress(br)
	if err != nil {
		return nil, err
	}
	structDec, _, err := NewStructDecompress(info, typ, br)
	if err != nil {
		return nil, err
	}

	for {
		_, err = tickDec.Decompress(br)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return nil, err
			}
		}
		_, err = structDec.Decompress(br)
		if err != nil {
			return nil, err
		}
	}
	// Now we have a decompressor with the correct state. We have to rewind the last bits used to indicate EOF

	tickC := tickDec.ToCompress()
	structC := structDec.ToCompress()

	return &CTickWriter{
		tickC:   tickC,
		structC: structC,
	}, nil
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

type CTickReader struct {
	Tick     uint64
	nextTick uint64
	Val      TickDeltas
	br       *compress.BReader
	tickC    *compress.TickDecompress
	structC  *StructDecompress
}

func NewCTickReader(info *ItemSection, typ reflect.Type, br *compress.BReader) (*CTickReader, error) {
	tickC, tick, err := compress.NewTickDecompress(br)
	if err != nil {
		return nil, err
	}
	structC, ptr, err := NewStructDecompress(info, typ, br)
	if err != nil {
		return nil, err
	}

	r := &CTickReader{
		Tick: tick,
		Val: TickDeltas{
			Pointer: ptr,
			Len:     1,
		},
		br:      br,
		tickC:   tickC,
		structC: structC,
	}

	// Read next tick
	r.nextTick, err = tickC.Decompress(br)
	if err != nil {
		if err == io.EOF {
			br.Rewind(5)
			return r, nil
		} else {
			return nil, err
		}
	}
	for r.Tick == r.nextTick {
		r.Val.Pointer, err = structC.Decompress(br)
		if err != nil {
			return nil, err
		}
		r.Val.Len += 1
		r.nextTick, err = tickC.Decompress(br)
		if err != nil {
			if err == io.EOF {
				br.Rewind(5)
				break
			} else {
				return nil, err
			}
		}
	}

	return r, nil
}

func (r *CTickReader) Next() error {
	// How can we know, here, if the tick was read last time ?
	// If nextTick is zero, we failed reading nextTick last time
	if r.nextTick == 0 {
		var err error
		r.nextTick, err = r.tickC.Decompress(r.br)
		if err != nil {
			if err == io.EOF {
				r.br.Rewind(5)
			}
			return err
		}
	}
	r.structC.Clear()
	r.Val.Len = 0
	r.Tick = r.nextTick
	var err error
	for r.Tick == r.nextTick {
		r.Val.Pointer, err = r.structC.Decompress(r.br)
		if err != nil {
			return err
		}
		r.Val.Len += 1
		r.nextTick, err = r.tickC.Decompress(r.br)
		if err != nil {
			if err == io.EOF {
				r.br.Rewind(5)
				break
			} else {
				return err
			}
		}
	}

	return nil
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
		var c compress.Compress
		switch f.Type {
		case UINT8, INT8:
			// TODO
			c = compress.NewUInt64Compress(*(*uint64)(unsafe.Pointer(ptr + uintptr(f.Offset))), bw)
		case INT64, UINT64, FLOAT64:
			c = compress.NewUInt64Compress(*(*uint64)(unsafe.Pointer(ptr + uintptr(f.Offset))), bw)
		default:
			panic("compression not supported")
		}
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

func NewStructDecompress(info *ItemSection, typ reflect.Type, br *compress.BReader) (*StructDecompress, unsafe.Pointer, error) {
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
		var err error
		var d compress.Decompress
		switch f.Type {
		case UINT8, INT8:
			ptr := ptr + uintptr(f.Offset)
			// TODO
			d, err = compress.NewUInt64Decompress(br, (*uint64)(unsafe.Pointer(ptr)))
			if err != nil {
				return nil, nil, err
			}
		case INT64, UINT64, FLOAT64:
			ptr := ptr + uintptr(f.Offset)
			d, err = compress.NewUInt64Decompress(br, (*uint64)(unsafe.Pointer(ptr)))
			if err != nil {
				return nil, nil, err
			}

		default:
			panic("compression not supported")
		}
		sd.readers = append(sd.readers, FieldReader{
			offset: uintptr(f.Offset),
			d:      d,
		})
	}
	sd.offset += size

	return sd, uptr, nil
}

func (d *StructDecompress) Decompress(br *compress.BReader) (unsafe.Pointer, error) {
	if int(d.offset) == cap(d.val) {
		// Need to increase the buffer
		val := make([]byte, 2*d.offset, 2*d.offset)
		copy(val, d.val)
		d.val = val
		d.uptr = unsafe.Pointer(&val[0])
	}
	for _, r := range d.readers {
		uptr := unsafe.Pointer(uintptr(d.uptr) + d.offset + r.offset)
		if err := r.d.Decompress(br, (*uint64)(uptr)); err != nil {
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