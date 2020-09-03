package compress

import (
	"fmt"
	"io"
)

type TickCompress struct {
	lastVal   uint64
	lastDelta int64
}

func NewTickCompress(start uint64, bw *BBuffer) *TickCompress {
	bw.WriteBits(start, 64)
	return &TickCompress{
		lastVal:   start,
		lastDelta: 0,
	}
}

func (c *TickCompress) Compress(tick uint64, bw *BBuffer) {
	delta := int64(tick - c.lastVal)
	dod := delta - c.lastDelta
	switch {
	case dod == 0:
		bw.WriteBit(Zero)
	case -63 <= dod && dod <= 64:
		bw.WriteBits(0x02, 2) // '10'
		bw.WriteBits(uint64(dod), 7)
	case -255 <= dod && dod <= 256:
		bw.WriteBits(0x06, 3) // '110'
		bw.WriteBits(uint64(dod), 9)
	case -2047 <= dod && dod <= 2048:
		bw.WriteBits(0x0e, 4) // '1110'
		bw.WriteBits(uint64(dod), 12)
	default:
		bw.WriteBits(0x1e, 5) // '11110'
		bw.WriteBits(uint64(dod), 32)
	}
	c.lastVal = tick
	c.lastDelta = delta
}

func (c *TickCompress) Open(bw *BBuffer) error {
	// Rewind 5 bits
	_ = bw.Rewind(5)
	return nil
}

func (c *TickCompress) Close(bw *BBuffer) {
	bw.WriteBits(0x1f, 5)
}

type TickDecompress struct {
	lastVal   uint64
	lastDelta int64
}

func NewTickDecompress(br *BReader) (*TickDecompress, uint64, error) {
	t, err := br.ReadBits(64)
	if err != nil {
		return nil, 0, err
	}
	td := &TickDecompress{
		lastVal:   t,
		lastDelta: 0,
	}
	return td, t, nil
}

func (c *TickDecompress) Decompress(br *BReader) (uint64, error) {
	var d byte
	for i := 0; i < 5; i++ {
		d <<= 1
		bit, err := br.ReadBit()
		if err != nil {
			if err == io.EOF {
				return 0, io.ErrUnexpectedEOF
			} else {
				return 0, err
			}
		}
		if bit == Zero {
			break
		}
		d |= 1
	}

	var size uint = 0
	var dod int64 = 0

	switch d {
	case 0x00:
		size = 0
	case 0x02:
		// read 7 bits
		size = 7
	case 0x06:
		// read 9 bits
		size = 9
	case 0x0e:
		// read 12 bits
		size = 12
	case 0x1e:
		// read 32 bits
		size = 32
	case 0x1f:
		// EOF
		return 0, io.EOF
	default:
		return 0, fmt.Errorf("unknown size flag: %d", d)
	}
	if size != 0 {
		bits, err := br.ReadBits(int(size))
		if err != nil {
			return 0, err
		}
		if bits > (1 << (size - 1)) {
			// or something
			bits = bits - (1 << size)
		}
		dod = int64(bits)
	}

	c.lastDelta = c.lastDelta + dod
	c.lastVal = c.lastVal + uint64(c.lastDelta)
	return c.lastVal, nil
}

func (c *TickDecompress) ToCompress() *TickCompress {
	return &TickCompress{
		lastVal:   c.lastVal,
		lastDelta: c.lastDelta,
	}
}
