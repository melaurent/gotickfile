package compress

import (
	"io"
	"sync"
)

// bstream is a stream of bits
type BBuffer struct {
	sync.RWMutex
	b     []byte // data
	count uint8  // how many bits are valid in current byte
}

func NewBBuffer(b []byte, c uint8) *BBuffer {
	return &BBuffer{b: b, count: c}
}

func (b *BBuffer) Clone() *BBuffer {
	d := make([]byte, len(b.b))
	copy(d, b.b)
	return &BBuffer{b: d, count: b.count}
}

func (b *BBuffer) CloneTip(size int) *BBuffer {
	l := len(b.b)
	if l == 0 {
		return &BBuffer{b: nil, count: 0}
	} else {
		from := l - size
		if from < 0 {
			from = 0
		}
		d := make([]byte, l-from, l-from)
		for i := from; i < l; i++ {
			d[i-from] = b.b[i]
		}
		return &BBuffer{b: d, count: b.count}
	}
}

func (b *BBuffer) TrimTip(size int) {
	for len(b.b) > 0 && size > 0 {
		b.b = b.b[:len(b.b)-1]
		b.count = 0
		size -= 1
	}
}

func (b *BBuffer) Bytes() []byte {
	return b.b
}

type bit bool

const (
	Zero bit = false
	One  bit = true
)

func (b *BBuffer) WriteBit(bit bit) {
	if b.count == 0 {
		b.b = append(b.b, 0)
		b.count = 8
	}

	i := len(b.b) - 1

	if bit {
		b.b[i] |= 1 << (b.count - 1)
	}

	b.count--
}

func (b *BBuffer) WriteBytes(bytes []byte) {
	for _, byt := range bytes {
		b.WriteByte(byt)
	}
}

func (b *BBuffer) WriteByte(byt byte) {

	if b.count == 0 {
		b.b = append(b.b, byt)
		return
	}

	i := len(b.b) - 1

	// fill up b.b with b.count bits from byt
	b.b[i] |= byt >> (8 - b.count)

	b.b = append(b.b, 0)
	i++
	b.b[i] = byt << b.count
}

func (b *BBuffer) WriteBits(u uint64, nbits int) {

	u <<= (64 - uint(nbits))
	for nbits >= 8 {
		byt := byte(u >> 56)
		b.WriteByte(byt)
		u <<= 8
		nbits -= 8
	}

	for nbits > 0 {
		b.WriteBit((u >> 63) == 1)
		u <<= 1
		nbits--
	}
}

func (b *BBuffer) Rewind(offset int) error {
	for offset >= int(8-b.count) {
		offset -= int(8 - b.count)
		// We have b.count bit written in the last byte
		// what we have to do is remove last byte
		b.b = b.b[:len(b.b)-1]
		b.count = 0
	}

	i := len(b.b) - 1
	b.b[i] = b.b[i] & (0xFF << ((b.count) + uint8(offset)))
	b.count += uint8(offset)

	return nil
}

type BReader struct {
	buffer *BBuffer
	count  uint8
	idx    int
}

func NewBReader(buf *BBuffer) *BReader {
	return &BReader{
		buffer: buf,
		idx:    0,
		count:  8,
	}
}

func (b *BReader) End() bool {
	b.buffer.RLock()
	defer b.buffer.RUnlock()
	N := len(b.buffer.b)
	if N == 0 {
		return true
	}
	return (b.idx == N-1) && (b.count == b.buffer.count)
}

func (b *BReader) Bytes() []byte {
	return b.buffer.Bytes()
}

func (b *BReader) Rewind(nbits uint) {
	for nbits >= 8 {
		b.idx -= 1
		nbits -= 8
	}
	if uint(8-b.count) >= nbits {
		b.count += uint8(nbits) // We add nbits bit to read again
	} else {
		b.idx -= 1
		nbits -= 8 - uint(b.count)
		b.count = uint8(nbits)
	}
}

func (b *BReader) ReadBit() (bit, error) {

	if len(b.buffer.b) == b.idx {
		return false, io.EOF
	}

	if b.count == 0 {
		b.idx += 1
		// did we just run out of stuff to read?
		if len(b.buffer.b) == b.idx {
			return false, io.EOF
		}
		b.count = 8
	}
	byt := b.buffer.b[b.idx] << (8 - b.count)

	b.count--
	d := byt & 0x80
	return d != 0, nil
}

func (b *BReader) ReadByte() (byte, error) {

	if len(b.buffer.b) == b.idx {
		return 0, io.EOF
	}

	if b.count == 0 {
		b.idx += 1
		if len(b.buffer.b) == b.idx {
			return 0, io.EOF
		}
		b.count = 8
	}
	byt := b.buffer.b[b.idx] << (8 - b.count)
	if b.count == 8 {
		b.count = 0
		return byt, nil
	}

	byt2 := byt
	b.idx += 1
	if len(b.buffer.b) == b.idx {
		return 0, io.EOF
	}
	byt = b.buffer.b[b.idx]

	byt2 |= byt >> b.count
	byt <<= (8 - b.count)

	return byt2, nil
}

func (b *BReader) ReadBits(nbits int) (uint64, error) {

	var u uint64

	for nbits >= 8 {
		byt, err := b.ReadByte()
		if err != nil {
			return 0, err
		}

		u = (u << 8) | uint64(byt)
		nbits -= 8
	}

	if nbits == 0 {
		return u, nil
	}

	byt := b.buffer.b[b.idx] << (8 - b.count)

	if nbits > int(b.count) {
		u = (u << uint(b.count)) | uint64(byt>>(8-b.count))
		nbits -= int(b.count)
		b.idx += 1
		if len(b.buffer.b) == b.idx {
			return 0, io.EOF
		}
		b.count = 8
		byt = b.buffer.b[b.idx]
	}

	u = (u << uint(nbits)) | uint64(byt>>(8-uint(nbits)))
	byt <<= uint(nbits)
	b.count -= uint8(nbits)
	return u, nil
}
