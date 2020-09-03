package compress

import (
	"math/bits"
)

type Float64Compress struct {
	lastVal  uint64
	leading  uint8
	trailing uint8
}

func NewFloat64Compress(val uint64, bw *BBuffer) *UInt64Compress {
	bw.WriteBits(val, 64)
	return &UInt64Compress{
		lastVal: val,
	}
}

func (c *Float64Compress) Compress(val uint64, bw *BBuffer) {
	xor := val ^ c.lastVal
	if xor == 0 {
		bw.WriteBit(Zero)
	} else {
		bw.WriteBit(One)

		leading := uint8(bits.LeadingZeros64(xor))
		trailing := uint8(bits.TrailingZeros64(xor))
		// clamp number of leading zeros to avoid overflow when encoding
		if leading >= 32 {
			leading = 31
		}

		if c.leading != ^uint8(0) && leading >= c.leading && trailing >= c.trailing {
			bw.WriteBit(Zero)
			bw.WriteBits(xor>>c.trailing, 64-int(c.leading)-int(c.trailing))
		} else {
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

type Float64Decompress struct {
	val      *uint64
	leading  uint8
	trailing uint8
}

func NewFloat64Decompress(br *BReader, ptr *uint64) (*Float64Decompress, error) {
	val, err := br.ReadBits(64)
	if err != nil {
		return nil, err
	}
	*ptr = val
	return &Float64Decompress{
		val:      ptr,
		leading:  0,
		trailing: 0,
	}, nil
}

func (d *Float64Decompress) Decompress(br *BReader) error {
	// read compressed value
	bit, err := br.ReadBit()
	if err != nil {
		return err
	}

	if bit == Zero {
		// it.val = it.val
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
		*d.val = *d.val ^ (bits << d.trailing)
	}

	return nil
}

func (d *Float64Decompress) ToFloat64Compress() *Float64Compress {
	return &Float64Compress{
		lastVal:  *d.val,
		leading:  d.leading,
		trailing: d.trailing,
	}
}
