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

func NewCTickWriter(info *ItemSection, tick uint64, val reflect.Value, bw *compress.BBuffer) *CTickWriter {
	ctw := &CTickWriter{
		tickC:   compress.NewTickCompress(tick, bw),
		structC: NewStructCompress(info, val, bw),
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
		err = structDec.Decompress(br)
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

func (w *CTickWriter) Write(tick uint64, val reflect.Value, bw *compress.BBuffer) {
	w.tickC.Compress(tick, bw)
	w.structC.Compress(val, bw)
}

func (w *CTickWriter) Open(bw *compress.BBuffer) error {
	return w.tickC.Open(bw)
}

func (w *CTickWriter) Close(bw *compress.BBuffer) {
	w.tickC.Close(bw)
}

type CTickReader struct {
	Tick    uint64
	Val     interface{}
	br      *compress.BReader
	tickC   *compress.TickDecompress
	structC *StructDecompress
}

func NewCTickReader(info *ItemSection, typ reflect.Type, br *compress.BReader) (*CTickReader, error) {
	tickC, tick, err := compress.NewTickDecompress(br)
	if err != nil {
		return nil, err
	}
	structC, val, err := NewStructDecompress(info, typ, br)
	if err != nil {
		return nil, err
	}
	return &CTickReader{
		Tick:    tick,
		Val:     val,
		br:      br,
		tickC:   tickC,
		structC: structC,
	}, nil
}

func (r *CTickReader) Next() error {
	var err error
	r.Tick, err = r.tickC.Decompress(r.br)

	if err != nil {
		if err == io.EOF {
			// Rewind
			r.br.Rewind(5)
		}
		return err
	}
	err = r.structC.Decompress(r.br)
	if err != nil {
		return err
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

func NewStructCompress(info *ItemSection, val reflect.Value, bw *compress.BBuffer) *StructCompress {
	sc := &StructCompress{
		writers: nil,
	}
	for _, f := range info.Fields {
		var c compress.Compress
		switch f.Type {
		case UINT8, INT8:
			ptr := val.Pointer() + uintptr(f.Offset)
			// TODO
			c = compress.NewUInt64Compress(*(*uint64)(unsafe.Pointer(ptr)), bw)
		case INT64, UINT64, FLOAT64:
			ptr := val.Pointer() + uintptr(f.Offset)
			c = compress.NewUInt64Compress(*(*uint64)(unsafe.Pointer(ptr)), bw)
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

func (c *StructCompress) Compress(val reflect.Value, bw *compress.BBuffer) {
	ptr := val.Pointer()
	for _, w := range c.writers {
		w.c.Compress(*(*uint64)(unsafe.Pointer(ptr + w.offset)), bw)
	}
}

type StructDecompress struct {
	readers []FieldReader
	val     reflect.Value
}

func NewStructDecompress(info *ItemSection, typ reflect.Type, br *compress.BReader) (*StructDecompress, interface{}, error) {
	sc := &StructDecompress{
		readers: nil,
	}
	sc.val = reflect.New(typ)

	for _, f := range info.Fields {
		var err error
		var d compress.Decompress
		switch f.Type {
		case UINT8, INT8:
			ptr := sc.val.Pointer() + uintptr(f.Offset)
			// TODO
			d, err = compress.NewUInt64Decompress(br, (*uint64)(unsafe.Pointer(ptr)))
			if err != nil {
				return nil, nil, err
			}
		case INT64, UINT64, FLOAT64:
			ptr := sc.val.Pointer() + uintptr(f.Offset)
			d, err = compress.NewUInt64Decompress(br, (*uint64)(unsafe.Pointer(ptr)))
			if err != nil {
				return nil, nil, err
			}
		default:
			panic("compression not supported")
		}
		sc.readers = append(sc.readers, FieldReader{
			offset: uintptr(f.Offset),
			d:      d,
		})
	}

	return sc, sc.val.Interface(), nil
}

func (d *StructDecompress) Decompress(br *compress.BReader) error {
	for _, r := range d.readers {
		if err := r.d.Decompress(br); err != nil {
			return err
		}
	}
	return nil
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
