package compress

import "unsafe"

type Compress interface {
	Compress(uint64, *BBuffer)
}

type Decompress interface {
	Decompress(*BitReader, unsafe.Pointer) error
	ToCompress() Compress
}

const (
	UINT32_GORILLA_COMPRESS uint8 = 0
	UINT64_GORILLA_COMPRESS uint8 = 1
	UINT8_GORILLA_COMPRESS  uint8 = 2
)

func GetCompress(val uint64, bw *BBuffer, version uint8) Compress {
	switch version {
	case UINT8_GORILLA_COMPRESS:
		return NewUInt8GorillaCompress(val, bw)
	case UINT32_GORILLA_COMPRESS:
		return NewUInt32GorillaCompress(val, bw)
	case UINT64_GORILLA_COMPRESS:
		return NewUInt64GorillaCompress(val, bw)
	default:
		return nil
	}
}

func GetDecompress(br *BitReader, ptr unsafe.Pointer, version uint8) (Decompress, error) {
	switch version {
	case UINT8_GORILLA_COMPRESS:
		return NewUInt8GorillaDecompress(br, ptr)
	case UINT32_GORILLA_COMPRESS:
		return NewUInt32GorillaDecompress(br, ptr)
	case UINT64_GORILLA_COMPRESS:
		return NewUInt64GorillaDecompress(br, ptr)
	default:
		return nil, nil
	}
}
