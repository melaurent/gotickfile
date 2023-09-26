package compress

import (
	"fmt"
	"math/rand"
	"testing"
	"unsafe"
)

func TestBytes32Compress(t *testing.T) {
	buf := NewBBuffer(nil, 0)
	var gvals [][32]byte
	N := 1000
	for i := 0; i < N; i++ {
		var v [32]byte
		for j := 0; j < 10; j++ {
			v[j] = uint8(rand.Uint32())
		}
		gvals = append(gvals, v)
	}
	c := NewBytes32RunLengthByteCompress(buf, gvals[0])
	for i := 1; i < len(gvals); i++ {
		c.Compress(buf, unsafe.Pointer(&gvals[i]))
	}

	// Compression ratio on noise is terrible.
	r := float64(len(gvals)*32) / float64(len(buf.b))
	fmt.Println(r)

	reader := NewBitReader(buf)
	var val [32]byte
	d, err := NewBytes32RunLengthByteDecompress(reader, unsafe.Pointer(&val))
	if err != nil {
		t.Fatal(err)
	}
	// compare
	if val != gvals[0] {
		t.Fatalf("different val: %x %x", val, gvals[0])
	}
	for i := 1; i < len(gvals); i++ {
		if err := d.Decompress(reader, unsafe.Pointer(&val)); err != nil {
			t.Fatal(err)
		}
		if val != gvals[i] {
			t.Fatal(err)
		}
	}
}
