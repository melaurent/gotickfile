package compress

import (
	"io"
	"sync"
)

type bit bool

const (
	Zero bit = false
	One  bit = true
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

func (b *BBuffer) Count() uint8 {
	return b.count
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

	u <<= 64 - uint(nbits)
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

type ChunkReader struct {
	buffer    *BBuffer
	count     uint8
	idx       int
	chunkSize int
}

func NewChunkReader(buf *BBuffer, chunckSize int) *ChunkReader {
	return &ChunkReader{
		buffer:    buf,
		count:     8,
		idx:       0,
		chunkSize: chunckSize,
	}
}

func (r *ChunkReader) ReadChunk() []byte {
	r.buffer.RLock()
	N := len(r.buffer.b)
	count := r.buffer.count
	r.buffer.RUnlock()

	if (r.idx == N-1) && (r.count == count) {
		return nil
	}
	// No bit left to read at our index, next
	if r.count == 0 {
		r.idx += 1
	}
	M := N - r.idx
	if M > r.chunkSize {
		M = r.chunkSize
	}
	// We can read
	chunk := make([]byte, M+1, M+1)
	r.buffer.RLock()
	for i := 0; i < M; i++ {
		chunk[i+1] = r.buffer.b[r.idx+i]
	}
	r.idx += M - 1
	if r.idx == len(r.buffer.b)-1 {
		// If we are at the end, we have the count of the buffer
		r.count = r.buffer.count
	} else {
		r.count = 0
	}
	chunk[0] = r.count

	r.buffer.RUnlock()

	return chunk
}

type ChunkWriter struct {
	buffer *BBuffer
}

func NewChunkWriter(buf *BBuffer) *ChunkWriter {
	return &ChunkWriter{
		buffer: buf,
	}
}

func (w *ChunkWriter) WriteChunk(chunk []byte) {
	w.buffer.Lock()
	for i := 1; i < len(chunk); i++ {
		// count is bits left to write
		if w.buffer.count == 0 {
			w.buffer.b = append(w.buffer.b, chunk[i])
		} else {
			w.buffer.b[len(w.buffer.b)-1] = chunk[i]
			w.buffer.count = 0
		}
	}
	w.buffer.count = chunk[0]
	w.buffer.Unlock()
}

type BitReader struct {
	buffer *BBuffer
	count  uint8
	idx    int
}

type BitReaderState struct {
	idx   int
	count uint8
}

func NewBitReader(buf *BBuffer) *BitReader {
	return &BitReader{
		buffer: buf,
		idx:    0,
		count:  8,
	}
}

func (b *BitReader) State() BitReaderState {
	return BitReaderState{
		idx:   b.idx,
		count: b.count,
	}
}

func (b *BitReader) Reset(state BitReaderState) {
	b.idx = state.idx
	b.count = state.count
}

func (b *BitReader) End() bool {
	b.buffer.RLock()
	defer b.buffer.RUnlock()
	N := len(b.buffer.b)
	if N == 0 {
		return true
	}
	return (b.idx == N-1) && (b.count == b.buffer.count)
}

func (b *BitReader) Bytes() []byte {
	return b.buffer.Bytes()
}

func (b *BitReader) Rewind(nbits uint) {
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

func (b *BitReader) ReadBit() (bit, error) {

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

func (b *BitReader) ReadByte() (byte, error) {

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
	byt <<= 8 - b.count

	return byt2, nil
}

func (b *BitReader) ReadBytes(n int) ([]byte, error) {
	return b.ReadBytesInto(make([]byte, n))
}

func (b *BitReader) ReadBytesInto(res []byte) ([]byte, error) {
	if len(b.buffer.b) == b.idx {
		return nil, io.EOF
	}
	if b.count == 0 {
		b.idx += 1
		if len(b.buffer.b) == b.idx {
			return nil, io.EOF
		}
		b.count = 8
	}
	if b.count == 8 {
		// It's our lucky day, no bit operation needed
		for i := 0; i < len(res); i++ {
			if len(b.buffer.b) == b.idx {
				return nil, io.EOF
			}
			res[i] = b.buffer.b[b.idx]
			b.idx += 1
		}
		return res, nil
	} else {
		// Need to do some butchering
		for i := 0; i < len(res); i++ {
			if b.idx >= len(b.buffer.b)-1 {
				return nil, io.EOF
			}
			res[i] = b.buffer.b[b.idx] << (8 - b.count)
			b.idx += 1
			res[i] |= b.buffer.b[b.idx] >> b.count
		}
		// b.count is still correct has we only read full bytes
		return res, nil
	}
}

func (b *BitReader) ReadBits(nbits int) (uint64, error) {

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
