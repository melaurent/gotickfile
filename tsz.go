package gotickfile

import (
	"fmt"
	"io"
)

func CompressTicks(ticks []uint64) []byte {

	if len(ticks) == 0 {
		return nil
	}

	bw := newBWriter()
	bw.writeBits(ticks[0], 64)

	if len(ticks) == 1 {
		return bw.stream
	}

	tDelta := ticks[1] - ticks[0]
	bw.writeBits(tDelta, 64)

	for i := 2; i < len(ticks); i++ {
		dod := int64(ticks[i] - ticks[i-1]) - int64(tDelta)
		switch {
		case dod == 0:
			bw.writeBit(zero)
		case -63 <= dod && dod <= 64:
			bw.writeBits(0x02, 2) // '10'
			bw.writeBits(uint64(dod), 7)
		case -255 <= dod && dod <= 256:
			bw.writeBits(0x06, 3) // '110'
			bw.writeBits(uint64(dod), 9)
		case -2047 <= dod && dod <= 2048:
			bw.writeBits(0x0e, 4) // '1110'
			bw.writeBits(uint64(dod), 12)
		default:
			bw.writeBits(0x0f, 4) // '1111'
			bw.writeBits(uint64(dod), 32)
		}

		tDelta = ticks[i] - ticks[i-1]
	}

	return bw.stream
}

func DecompressTicks(bytes []byte, count int) ([]uint64, error) {
	if count == 0 {
		return nil, nil
	}

	br := newBReader(bytes)

	ticks := make([]uint64, count)
	t, err := br.readBits(64)
	if err != nil {
		return nil, err
	}
	ticks[0] = t

	if count == 1 {
		return ticks, nil
	}

	tDelta, err := br.readBits(64)
	if err != nil {
		return nil, err
	}
	ticks[1] = ticks[0] + tDelta

	for i := 2; i < count; i++ {
		var d byte
		for i := 0; i < 4; i++ {
			d <<= 1
			bit, err := br.readBit()
			if err != nil {
				panic(err)
			}
			if bit == zero {
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
		case 0x0f:
			// read 32 bits
			size = 32
		default:
			return nil, fmt.Errorf("unknown size flag: %d", d)
		}
		if size != 0 {
			bits, err := br.readBits(int(size))
			if err != nil {
				return nil, err
			}
			if bits > (1 << (size - 1)) {
				// or something
				bits = bits - (1 << size)
			}
			dod = int64(bits)
		}


		tDelta = tDelta + uint64(dod)
		ticks[i] = ticks[i-1] + tDelta
	}

	return ticks, nil
}

// bstream is a stream of bits
type bstream struct {
	// the data stream
	stream []byte

	// how many bits are valid in current byte
	count uint8

	// current byte
	byte uint8
}

func newBReader(b []byte) *bstream {
	return &bstream{stream: b, count: 8, byte: b[0]}
}

func newBWriter() *bstream {
	return &bstream{stream: []byte{}, count: 0}
}

func (b *bstream) clone() *bstream {
	d := make([]byte, len(b.stream))
	copy(d, b.stream)
	return &bstream{stream: d, count: b.count}
}

func (b *bstream) bytes() []byte {
	return b.stream
}

type bit bool

const (
	zero bit = false
	one  bit = true
)

func (b *bstream) writeBit(bit bit) {

	if b.count == 0 {
		b.stream = append(b.stream, 0)
		b.count = 8
	}

	i := len(b.stream) - 1

	if bit {
		b.stream[i] |= 1 << (b.count - 1)
	}

	b.count--
}

func (b *bstream) writeByte(byt byte) {

	if b.count == 0 {
		b.stream = append(b.stream, 0)
		b.count = 8
	}

	i := len(b.stream) - 1

	// fill up b.b with b.count bits from byt
	b.stream[i] |= byt >> (8 - b.count)

	b.stream = append(b.stream, 0)
	i++
	b.stream[i] = byt << b.count
}

func (b *bstream) writeBits(u uint64, nbits int) {
	u <<= (64 - uint(nbits))
	for nbits >= 8 {
		byt := byte(u >> 56)
		b.writeByte(byt)
		u <<= 8
		nbits -= 8
	}

	for nbits > 0 {
		b.writeBit((u >> 63) == 1)
		u <<= 1
		nbits--
	}
}

func (b *bstream) readBit() (bit, error) {

	if len(b.stream) == 0 {
		return false, io.EOF
	}

	if b.count == 0 {
		b.stream = b.stream[1:]
		// did we just run out of stuff to read?
		if len(b.stream) == 0 {
			return false, io.EOF
		}
		b.count = 8
		b.byte = b.stream[0]
	}

	b.count--
	d := b.byte & 0x80
	b.byte <<= 1
	return d != 0, nil
}

func (b *bstream) readByte() (byte, error) {

	if len(b.stream) == 0 {
		return 0, io.EOF
	}

	if b.count == 0 {
		b.stream = b.stream[1:]

		if len(b.stream) == 0 {
			return 0, io.EOF
		}

		b.count = 8
		b.byte = b.stream[0]
	}

	if b.count == 8 {
		b.count = 0
		return b.byte, nil
	}

	byt := b.byte
	b.stream = b.stream[1:]
	if len(b.stream) == 0 {
		return 0, io.EOF
	}
	b.byte = b.stream[0]

	byt |= b.byte >> b.count
	b.byte <<= (8 - b.count)

	return byt, nil
}

func (b *bstream) readBits(nbits int) (uint64, error) {

	var u uint64

	for nbits >= 8 {
		byt, err := b.readByte()
		if err != nil {
			return 0, err
		}

		u = (u << 8) | uint64(byt)
		nbits -= 8
	}

	if nbits == 0 {
		return u, nil
	}

	if nbits > int(b.count) {
		u = (u << uint(b.count)) | uint64(b.byte>>(8-b.count))
		nbits -= int(b.count)
		b.stream = b.stream[1:]

		if len(b.stream) == 0 {
			return 0, io.EOF
		}
		b.count = 8
		b.byte = b.stream[0]
	}

	u = (u << uint(nbits)) | uint64(b.byte>>(8-uint(nbits)))
	b.byte <<= uint(nbits)
	b.count -= uint8(nbits)
	return u, nil
}