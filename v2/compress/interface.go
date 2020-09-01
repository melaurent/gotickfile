package compress

type Compress interface {
	Compress(uint64, *BBuffer)
}

type Decompress interface {
	Decompress(*BReader) error
	ToCompress() Compress
}
