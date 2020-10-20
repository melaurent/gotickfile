package compress

type Compress interface {
	Compress(uint64, *BBuffer)
}

type Decompress interface {
	Decompress(*BitReader, *uint64) error
	ToCompress() Compress
}

const (
	UINT64_GORILLA_COMPRESS uint8 = 1
)

func GetCompress(val uint64, bw *BBuffer, version uint8) Compress {
	switch version {
	case UINT64_GORILLA_COMPRESS:
		return NewUInt64GorillaCompress(val, bw)
	default:
		return nil
	}
}

func GetDecompress(br *BitReader, ptr *uint64, version uint8) (Decompress, error) {
	switch version {
	case UINT64_GORILLA_COMPRESS:
		return NewUInt64GorillaDecompress(br, ptr)
	default:
		return nil, nil
	}
}
