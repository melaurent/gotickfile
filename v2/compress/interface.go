package compress

type Compress interface {
	Compress(uint64, *BBuffer)
}

type Decompress interface {
	Decompress(*BReader, *uint64) error
	ToCompress() Compress
}
