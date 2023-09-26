package compress

import (
	"bytes"
	"unsafe"
)

type Bytes32RunLengthByteCompress struct {
	lastVal [32]byte
}

func NewBytes32RunLengthByteCompress(bw *BBuffer, val [32]byte) *Bytes32RunLengthByteCompress {
	bw.WriteBytes(val[:])
	return &Bytes32RunLengthByteCompress{
		lastVal: val,
	}
}

func (c *Bytes32RunLengthByteCompress) Compress(bw *BBuffer, bi unsafe.Pointer) {
	b := *(*[32]byte)(bi)
	xor := make([]byte, len(b))
	for i := 0; i < len(b); i++ {
		xor[i] = b[i] ^ c.lastVal[i]
	}
	if bytes.Compare(xor, make([]byte, len(b))) == 0 {
		// Bytes are the same
		bw.WriteBit(Zero)
	} else {
		bw.WriteBit(One)

		var count uint8 = 1
		// count is in byte, the max count amount is 32 as it can be all ones
		for i := 1; i < len(b); i++ {
			if xor[i] == xor[i-1] {
				count += 1
			} else {
				bw.WriteByte(xor[i-1])
				bw.WriteBits(uint64(count), 5)
				count = 1
			}
		}
		if count == 32 {
			// 32 bytes of 1, special case
			bw.WriteByte(xor[len(xor)-1])
			bw.WriteBits(0, 5)
		} else {
			bw.WriteByte(xor[len(xor)-1])
			bw.WriteBits(uint64(count), 5)
		}
	}
	c.lastVal = b
}

type Bytes32RunLengthByteDecompress struct {
	lastVal [32]byte
}

func NewBytes32RunLengthByteDecompress(br *BitReader, ptr unsafe.Pointer) (*Bytes32RunLengthByteDecompress, error) {
	d := &Bytes32RunLengthByteDecompress{}
	_, err := br.ReadBytesInto(d.lastVal[:])
	if err != nil {
		return nil, err
	}
	val := (*[32]byte)(ptr)
	*val = d.lastVal
	return d, nil
}

func (d *Bytes32RunLengthByteDecompress) Decompress(br *BitReader, ptr unsafe.Pointer) error {
	bit, err := br.ReadBit()
	if err != nil {
		return err
	}
	val := (*[32]byte)(ptr)
	if bit == Zero {
		*val = d.lastVal
	} else {
		idx := 0
		for idx != 32 {
			// Decode Run-Length encoding
			byt, err := br.ReadByte()
			if err != nil {
				return err
			}
			count, err := br.ReadBits(5)
			if err != nil {
				return err
			}
			// Write byt count times
			for i := uint64(0); i < count; i++ {
				val[idx] = byt ^ d.lastVal[idx]
				idx += 1
			}
		}
		d.lastVal = *val
	}
	return nil
}

func (d *Bytes32RunLengthByteDecompress) ToCompress() Compress {
	return &Bytes32RunLengthByteCompress{lastVal: d.lastVal}
}

type Bytes256RunLengthByteCompress struct {
	lastVal [256]byte
}

func NewBytes256RunLengthByteCompress(bw *BBuffer, val [256]byte) *Bytes256RunLengthByteCompress {
	bw.WriteBytes(val[:])
	return &Bytes256RunLengthByteCompress{
		lastVal: val,
	}
}

func (c *Bytes256RunLengthByteCompress) Compress(bw *BBuffer, bi unsafe.Pointer) {
	b := *(*[256]byte)(bi)
	xor := make([]byte, len(b))
	for i := 0; i < len(b); i++ {
		xor[i] = b[i] ^ c.lastVal[i]
	}
	if bytes.Compare(xor, make([]byte, len(b))) == 0 {
		// Bytes are the same
		bw.WriteBit(Zero)
	} else {
		bw.WriteBit(One)

		var count uint32 = 1
		// count is in byte, the max count amount is 256 as it can be all ones
		for i := 1; i < len(b); i++ {
			if xor[i] == xor[i-1] {
				count += 1
			} else {
				bw.WriteByte(xor[i-1])
				bw.WriteByte(uint8(count))
				count = 1
			}
		}
		if count == 256 {
			// 32 bytes of 1, special case
			bw.WriteByte(xor[len(xor)-1])
			bw.WriteByte(0)
		} else {
			bw.WriteByte(xor[len(xor)-1])
			bw.WriteByte(uint8(count))
		}
	}
	c.lastVal = b
}

type Bytes256RunLengthByteDecompress struct {
	lastVal [256]byte
}

func NewBytes256RunLengthByteDecompress(br *BitReader, ptr unsafe.Pointer) (*Bytes256RunLengthByteDecompress, error) {
	d := &Bytes256RunLengthByteDecompress{}
	_, err := br.ReadBytesInto(d.lastVal[:])
	if err != nil {
		return nil, err
	}
	val := (*[256]byte)(ptr)
	*val = d.lastVal
	return d, nil
}

func (d *Bytes256RunLengthByteDecompress) Decompress(br *BitReader, ptr unsafe.Pointer) error {
	bit, err := br.ReadBit()
	if err != nil {
		return err
	}
	val := (*[256]byte)(ptr)
	if bit == Zero {
		*val = d.lastVal
	} else {
		idx := 0
		for idx != 256 {
			// Decode Run-Length encoding
			byt, err := br.ReadByte()
			if err != nil {
				return err
			}
			count, err := br.ReadBits(5)
			if err != nil {
				return err
			}
			if count == 0 {
				count = 256
			}
			// Write byt count times
			for i := uint64(0); i < count; i++ {
				val[idx] = byt ^ d.lastVal[idx]
				idx += 1
			}
		}
		d.lastVal = *val
	}
	return nil
}

func (d *Bytes256RunLengthByteDecompress) ToCompress() Compress {
	return &Bytes256RunLengthByteCompress{lastVal: d.lastVal}
}
