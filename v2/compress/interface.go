package compress

import "unsafe"

type Compress interface {
	Compress(*BBuffer, unsafe.Pointer)
}

type Decompress interface {
	Decompress(*BitReader, unsafe.Pointer) error
	ToCompress() Compress
}

const (
	Uint32GorillaCompressType         uint8 = 0
	Uint64GorillaCompressType         uint8 = 1
	Uint8GorillaCompressType          uint8 = 2
	Bytes32RunLengthByteCompressType  uint8 = 3
	Bytes256RunLengthByteCompressType uint8 = 4
	NoneCompressType                  uint8 = 5 // Cannot change, legacy..
)

func GetCompress(bw *BBuffer, val unsafe.Pointer, size uint32, version uint8) Compress {
	switch version {
	case NoneCompressType:
		return NewNoneCompress(bw, val, size)
	case Uint8GorillaCompressType:
		return NewUInt8GorillaCompress(bw, *(*uint64)(val))
	case Uint32GorillaCompressType:
		return NewUInt32GorillaCompress(bw, *(*uint64)(val))
	case Uint64GorillaCompressType:
		return NewUInt64GorillaCompress(bw, *(*uint64)(val))
	case Bytes32RunLengthByteCompressType:
		return NewBytes32RunLengthByteCompress(bw, *(*[32]byte)(val))
	case Bytes256RunLengthByteCompressType:
		return NewBytes256RunLengthByteCompress(bw, *(*[256]byte)(val))
	default:
		return nil
	}
}

func GetDecompress(br *BitReader, ptr unsafe.Pointer, size uint32, version uint8) (Decompress, error) {
	switch version {
	case NoneCompressType:
		return NewNoneDecompress(br, ptr, size)
	case Uint8GorillaCompressType:
		return NewUInt8GorillaDecompress(br, ptr)
	case Uint32GorillaCompressType:
		return NewUInt32GorillaDecompress(br, ptr)
	case Uint64GorillaCompressType:
		return NewUInt64GorillaDecompress(br, ptr)
	case Bytes32RunLengthByteCompressType:
		return NewBytes32RunLengthByteDecompress(br, ptr)
	case Bytes256RunLengthByteCompressType:
		return NewBytes256RunLengthByteDecompress(br, ptr)
	default:
		return nil, nil
	}
}
