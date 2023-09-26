package compress

import (
	"unsafe"
)

type NoneCompress struct {
	size uint32
}

func NewNoneCompress(bw *BBuffer, val unsafe.Pointer, size uint32) *NoneCompress {
	c := &NoneCompress{
		size: size,
	}
	c.Compress(bw, val)
	return c
}

func (c *NoneCompress) Compress(bw *BBuffer, val unsafe.Pointer) {
	for i := uint32(0); i < c.size; i++ {
		bw.WriteByte(*(*byte)(unsafe.Pointer(uintptr(val) + uintptr(i))))
	}
}

type NoneDecompress struct {
	size uint32
}

func NewNoneDecompress(br *BitReader, ptr unsafe.Pointer, size uint32) (*NoneDecompress, error) {
	d := &NoneDecompress{
		size: size,
	}
	if err := d.Decompress(br, ptr); err != nil {
		return nil, err
	}
	return d, nil
}

func (d *NoneDecompress) Decompress(br *BitReader, val unsafe.Pointer) error {
	b, err := br.ReadBytes(int(d.size))
	if err != nil {
		return err
	}
	for i := uint32(0); i < d.size; i++ {
		*(*byte)(unsafe.Pointer(uintptr(val) + uintptr(i))) = b[i]
	}
	return nil
}

func (d *NoneDecompress) ToCompress() Compress {
	return &NoneCompress{
		size: d.size,
	}
}
