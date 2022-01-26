package compress

import (
	"math/bits"
	"unsafe"
)

type UInt64GorillaCompress struct {
	lastVal  uint64
	leading  uint8
	trailing uint8
	bucket1  int
	bucket2  int
	bucket3  int
}

func NewUInt64GorillaCompress(val uint64, bw *BBuffer) *UInt64GorillaCompress {
	bw.WriteBits(val, 64)
	return &UInt64GorillaCompress{
		lastVal:  val,
		leading:  ^uint8(0),
		trailing: 0,
	}
}

func (c *UInt64GorillaCompress) Compress(val uint64, bw *BBuffer) {
	xor := val ^ c.lastVal
	if xor == 0 {
		bw.WriteBit(Zero)
		c.bucket1 += 1
	} else {
		bw.WriteBit(One)

		leading := uint8(bits.LeadingZeros64(xor))
		trailing := uint8(bits.TrailingZeros64(xor))
		// clamp number of leading zeros to avoid overflow when encoding
		if leading >= 32 {
			leading = 31
		}

		if c.leading != ^uint8(0) && leading >= c.leading && trailing >= c.trailing {
			c.bucket2 += 1
			bw.WriteBit(Zero)
			bw.WriteBits(xor>>c.trailing, 64-int(c.leading)-int(c.trailing))
		} else {
			c.bucket3 += 1
			c.leading, c.trailing = leading, trailing

			bw.WriteBit(One)
			bw.WriteBits(uint64(leading), 5)

			// Note that if leading == trailing == 0, then sigbits == 64.  But that value doesn't actually fit into the 6 bits we have.
			// Luckily, we never need to encode 0 significant bits, since that would put us in the other case (vdelta == 0).
			// So instead we write out a 0 and adjust it back to 64 on unpacking.
			sigbits := 64 - leading - trailing
			bw.WriteBits(uint64(sigbits), 6)
			bw.WriteBits(xor>>trailing, int(sigbits))
		}
		c.lastVal = val
	}
}

type UInt64GorillaDecompress struct {
	lastVal  uint64
	leading  uint8
	trailing uint8
}

func NewUInt64GorillaDecompress(br *BitReader, ptr unsafe.Pointer) (*UInt64GorillaDecompress, error) {
	val, err := br.ReadBits(64)
	if err != nil {
		return nil, err
	}
	*(*uint64)(ptr) = val
	return &UInt64GorillaDecompress{
		lastVal:  val,
		leading:  0,
		trailing: 0,
	}, nil
}

func (d *UInt64GorillaDecompress) Decompress(br *BitReader, ptr unsafe.Pointer) error {
	// read compressed value
	bit, err := br.ReadBit()
	if err != nil {
		return err
	}

	val := (*uint64)(ptr)
	if bit == Zero {
		*val = d.lastVal
	} else {
		bit, err := br.ReadBit()
		if err != nil {
			return err
		}
		if bit == Zero {
			// reuse leading/trailing zero bits
			// it.leading, it.trailing = it.leading, it.trailing
		} else {
			bits, err := br.ReadBits(5)
			if err != nil {
				return err
			}
			d.leading = uint8(bits)

			bits, err = br.ReadBits(6)
			if err != nil {
				return err
			}
			mbits := uint8(bits)
			// 0 significant bits here means we overflowed and we actually need 64; see comment in encoder
			if mbits == 0 {
				mbits = 64
			}
			d.trailing = 64 - d.leading - mbits
		}

		mbits := int(64 - d.leading - d.trailing)
		bits, err := br.ReadBits(mbits)
		if err != nil {
			return err
		}
		*val = d.lastVal ^ (bits << d.trailing)
		d.lastVal = *val
	}

	return nil
}

func (d *UInt64GorillaDecompress) ToCompress() Compress {
	return &UInt64GorillaCompress{
		lastVal:  d.lastVal,
		leading:  d.leading,
		trailing: d.trailing,
	}
}

type UInt32GorillaCompress struct {
	lastVal  uint32
	leading  uint8
	trailing uint8
	bucket1  int
	bucket2  int
	bucket3  int
}

func NewUInt32GorillaCompress(val uint64, bw *BBuffer) *UInt32GorillaCompress {
	bw.WriteBits(val, 32)
	return &UInt32GorillaCompress{
		lastVal:  uint32(val),
		leading:  ^uint8(0),
		trailing: 0,
	}
}

func (c *UInt32GorillaCompress) Compress(valu uint64, bw *BBuffer) {
	val := uint32(valu)
	xor := val ^ c.lastVal
	if xor == 0 {
		bw.WriteBit(Zero)
		c.bucket1 += 1
	} else {
		bw.WriteBit(One)

		leading := uint8(bits.LeadingZeros32(xor))
		trailing := uint8(bits.TrailingZeros32(xor))
		// clamp number of leading zeros to avoid overflow when encoding
		if leading >= 16 {
			leading = 15
		}

		if c.leading != ^uint8(0) && leading >= c.leading && trailing >= c.trailing {
			c.bucket2 += 1
			bw.WriteBit(Zero)
			bw.WriteBits(uint64(xor>>c.trailing), 32-int(c.leading)-int(c.trailing))
		} else {
			c.bucket3 += 1
			c.leading, c.trailing = leading, trailing

			bw.WriteBit(One)
			bw.WriteBits(uint64(leading), 4)

			// Note that if leading == trailing == 0, then sigbits == 64.  But that value doesn't actually fit into the 6 bits we have.
			// Luckily, we never need to encode 0 significant bits, since that would put us in the other case (vdelta == 0).
			// So instead we write out a 0 and adjust it back to 64 on unpacking.
			sigbits := 32 - leading - trailing
			bw.WriteBits(uint64(sigbits), 5)
			bw.WriteBits(uint64(xor>>trailing), int(sigbits))
		}
		c.lastVal = val
	}
}

type UInt32GorillaDecompress struct {
	lastVal  uint32
	leading  uint8
	trailing uint8
}

func NewUInt32GorillaDecompress(br *BitReader, ptr unsafe.Pointer) (*UInt32GorillaDecompress, error) {
	val, err := br.ReadBits(32)
	if err != nil {
		return nil, err
	}
	*(*uint32)(ptr) = uint32(val)
	return &UInt32GorillaDecompress{
		lastVal:  uint32(val),
		leading:  0,
		trailing: 0,
	}, nil
}

func (d *UInt32GorillaDecompress) Decompress(br *BitReader, ptr unsafe.Pointer) error {
	// read compressed value
	bit, err := br.ReadBit()
	if err != nil {
		return err
	}

	val := (*uint32)(ptr)
	if bit == Zero {
		*val = d.lastVal
	} else {
		bit, err := br.ReadBit()
		if err != nil {
			return err
		}
		if bit == Zero {
			// reuse leading/trailing zero bits
			// it.leading, it.trailing = it.leading, it.trailing
		} else {
			bits, err := br.ReadBits(4)
			if err != nil {
				return err
			}
			d.leading = uint8(bits)

			bits, err = br.ReadBits(5)
			if err != nil {
				return err
			}
			mbits := uint8(bits)
			// 0 significant bits here means we overflowed and we actually need 64; see comment in encoder
			if mbits == 0 {
				mbits = 32
			}
			d.trailing = 32 - d.leading - mbits
		}

		mbits := int(32 - d.leading - d.trailing)
		bits, err := br.ReadBits(mbits)
		if err != nil {
			return err
		}
		*val = d.lastVal ^ (uint32(bits) << d.trailing)
		d.lastVal = *val
	}

	return nil
}

func (d *UInt32GorillaDecompress) ToCompress() Compress {
	return &UInt32GorillaCompress{
		lastVal:  d.lastVal,
		leading:  d.leading,
		trailing: d.trailing,
	}
}

type UInt8GorillaCompress struct {
	lastVal  uint8
	leading  uint8
	trailing uint8
	bucket1  int
	bucket2  int
	bucket3  int
}

func NewUInt8GorillaCompress(val uint64, bw *BBuffer) *UInt8GorillaCompress {
	bw.WriteBits(val, 8)
	return &UInt8GorillaCompress{
		lastVal:  uint8(val),
		leading:  ^uint8(0),
		trailing: 0,
	}
}

func (c *UInt8GorillaCompress) Compress(valu uint64, bw *BBuffer) {
	val := uint8(valu)
	xor := val ^ c.lastVal
	if xor == 0 {
		bw.WriteBit(Zero)
		c.bucket1 += 1
	} else {
		bw.WriteBit(One)

		leading := uint8(bits.LeadingZeros8(xor))
		trailing := uint8(bits.TrailingZeros8(xor))
		// clamp number of leading zeros to avoid overflow when encoding
		if leading >= 4 {
			leading = 3
		}

		if c.leading != ^uint8(0) && leading >= c.leading && trailing >= c.trailing {
			c.bucket2 += 1
			bw.WriteBit(Zero)
			bw.WriteBits(uint64(xor>>c.trailing), 8-int(c.leading)-int(c.trailing))
		} else {
			c.bucket3 += 1
			c.leading, c.trailing = leading, trailing

			bw.WriteBit(One)
			bw.WriteBits(uint64(leading), 2)

			// Note that if leading == trailing == 0, then sigbits == 64.  But that value doesn't actually fit into the 6 bits we have.
			// Luckily, we never need to encode 0 significant bits, since that would put us in the other case (vdelta == 0).
			// So instead we write out a 0 and adjust it back to 64 on unpacking.
			sigbits := 8 - leading - trailing
			bw.WriteBits(uint64(sigbits), 3)
			bw.WriteBits(uint64(xor>>trailing), int(sigbits))
		}
		c.lastVal = val
	}
}

type UInt8GorillaDecompress struct {
	lastVal  uint8
	leading  uint8
	trailing uint8
}

func NewUInt8GorillaDecompress(br *BitReader, ptr unsafe.Pointer) (*UInt8GorillaDecompress, error) {
	val, err := br.ReadBits(8)
	if err != nil {
		return nil, err
	}
	*(*uint8)(ptr) = uint8(val)
	return &UInt8GorillaDecompress{
		lastVal:  uint8(val),
		leading:  0,
		trailing: 0,
	}, nil
}

func (d *UInt8GorillaDecompress) Decompress(br *BitReader, ptr unsafe.Pointer) error {
	// read compressed value
	bit, err := br.ReadBit()
	if err != nil {
		return err
	}

	val := (*uint8)(ptr)
	if bit == Zero {
		*val = d.lastVal
	} else {
		bit, err := br.ReadBit()
		if err != nil {
			return err
		}
		if bit == Zero {
			// reuse leading/trailing zero bits
			// it.leading, it.trailing = it.leading, it.trailing
		} else {
			bits, err := br.ReadBits(2)
			if err != nil {
				return err
			}
			d.leading = uint8(bits)

			bits, err = br.ReadBits(3)
			if err != nil {
				return err
			}
			mbits := uint8(bits)
			// 0 significant bits here means we overflowed and we actually need 64; see comment in encoder
			if mbits == 0 {
				mbits = 8
			}
			d.trailing = 8 - d.leading - mbits
		}

		mbits := int(8 - d.leading - d.trailing)
		bits, err := br.ReadBits(mbits)
		if err != nil {
			return err
		}
		*val = d.lastVal ^ (uint8(bits) << d.trailing)
		d.lastVal = *val
	}

	return nil
}

func (d *UInt8GorillaDecompress) ToCompress() Compress {
	return &UInt8GorillaCompress{
		lastVal:  d.lastVal,
		leading:  d.leading,
		trailing: d.trailing,
	}
}
