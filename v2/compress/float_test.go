package compress

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
)

type OBDelta struct {
	Part1 uint64
	Part2 uint64
}

func TestFloat64Compress(t *testing.T) {
	ts1 := []float64{0, 10, 15, 20, 25, 30, 35, 40, 40, 40, 41}
	buf := NewBBuffer(nil, 0)
	c := NewUInt64GorillaCompress(math.Float64bits(ts1[0]), buf)
	for i := 1; i < len(ts1); i++ {
		c.Compress(math.Float64bits(ts1[i]), buf)
	}
	fmt.Println(len(buf.b))
	reader := NewBitReader(buf)

	var val uint64
	dc, err := NewUInt64GorillaDecompress(reader, &val)
	if err != nil {
		t.Fatal(err)
	}
	if val != math.Float64bits(ts1[0]) {
		t.Fatalf("different val")
	}
	for i := 1; i < len(ts1); i++ {
		err = dc.Decompress(reader, &val)
		if err != nil {
			t.Fatal(err)
		}
		if val != math.Float64bits(ts1[i]) {
			t.Fatalf("different val %d %d", val, math.Float64bits(ts1[i]))
		}
	}

	ts2 := []uint64{42, 43, 45, 46}

	// Start writing again
	c = dc.ToCompress().(*UInt64GorillaCompress)

	for i := 0; i < len(ts2); i++ {
		c.Compress(ts2[i], buf)
	}

	reader = NewBitReader(buf)
	dc, err = NewUInt64GorillaDecompress(reader, &val)
	if err != nil {
		t.Fatal(err)
	}
	if val != math.Float64bits(ts1[0]) {
		t.Fatalf("different tick")
	}
	for i := 1; i < len(ts1); i++ {
		err = dc.Decompress(reader, &val)
		if err != nil {
			t.Fatal(err)
		}
		if val != math.Float64bits(ts1[i]) {
			t.Fatalf("different tick %d %f", val, ts1[i])
		}
	}
	for i := 0; i < len(ts2); i++ {
		err = dc.Decompress(reader, &val)
		if err != nil {
			t.Fatal(err)
		}
		if val != ts2[i] {
			t.Fatalf("different tick")
		}
	}
}

func TestFloat64CompressFuzz(t *testing.T) {
	N := 40000000
	var tmp float64 = 100000.
	ts := make([]float64, N)
	for i := 0; i < N; i++ {
		tmp += float64(rand.Int31n(2))
		ts[i] = tmp
	}
	buf := NewBBuffer(nil, 0)
	c := NewUInt64GorillaCompress(math.Float64bits(ts[0]), buf)
	for i := 1; i < len(ts); i++ {
		c.Compress(math.Float64bits(ts[i]), buf)
	}

	fmt.Println(float64(len(buf.b))/(8.*40000000.), c.bucket1, c.bucket2, c.bucket3)
	reader := NewBitReader(buf)

	var val uint64
	dc, err := NewUInt64GorillaDecompress(reader, &val)
	if err != nil {
		t.Fatal(err)
	}
	if val != math.Float64bits(ts[0]) {
		t.Fatalf("different first tick")
	}
	for i := 1; i < len(ts); i++ {
		err = dc.Decompress(reader, &val)
		if err != nil {
			t.Fatal(err)
		}
		if val != math.Float64bits(ts[i]) {
			t.Fatalf("different tick")
		}
	}
}

func TestFloat32Compress(t *testing.T) {
	ts1 := []float32{0, 10, 15, 20, 25, 30, 35, 40, 40, 40, 41}
	buf := NewBBuffer(nil, 0)
	c := NewUInt32GorillaCompress(uint64(math.Float32bits(ts1[0])), buf)
	for i := 1; i < len(ts1); i++ {
		c.Compress(uint64(math.Float32bits(ts1[i])), buf)
	}
	fmt.Println(len(buf.b))
	reader := NewBitReader(buf)

	var val uint64
	dc, err := NewUInt32GorillaDecompress(reader, &val)
	if err != nil {
		t.Fatal(err)
	}
	if val != uint64(math.Float32bits(ts1[0])) {
		t.Fatalf("different val")
	}
	for i := 1; i < len(ts1); i++ {
		err = dc.Decompress(reader, &val)
		if err != nil {
			t.Fatal(err)
		}
		if val != uint64(math.Float32bits(ts1[i])) {
			t.Fatalf("different val %d %d", val, math.Float32bits(ts1[i]))
		}
	}

	ts2 := []uint64{42, 43, 45, 46}

	// Start writing again
	c = dc.ToCompress().(*UInt32GorillaCompress)

	for i := 0; i < len(ts2); i++ {
		c.Compress(ts2[i], buf)
	}

	reader = NewBitReader(buf)
	dc, err = NewUInt32GorillaDecompress(reader, &val)
	if err != nil {
		t.Fatal(err)
	}
	if val != uint64(math.Float32bits(ts1[0])) {
		t.Fatalf("different tick")
	}
	for i := 1; i < len(ts1); i++ {
		err = dc.Decompress(reader, &val)
		if err != nil {
			t.Fatal(err)
		}
		if val != uint64(math.Float32bits(ts1[i])) {
			t.Fatalf("different tick %d %f", val, ts1[i])
		}
	}
	for i := 0; i < len(ts2); i++ {
		err = dc.Decompress(reader, &val)
		if err != nil {
			t.Fatal(err)
		}
		if val != ts2[i] {
			t.Fatalf("different tick")
		}
	}
}

func TestFloat32CompressFuzz(t *testing.T) {
	N := 40000000
	var tmp float32 = 100000.
	ts := make([]float32, N)
	for i := 0; i < N; i++ {
		tmp += float32(rand.Int31n(2))
		ts[i] = tmp
	}
	buf := NewBBuffer(nil, 0)
	c := NewUInt32GorillaCompress(uint64(math.Float32bits(ts[0])), buf)
	for i := 1; i < len(ts); i++ {
		c.Compress(uint64(math.Float32bits(ts[i])), buf)
	}

	fmt.Println(float64(len(buf.b))/(4.*40000000.), c.bucket1, c.bucket2, c.bucket3)
	reader := NewBitReader(buf)

	var val uint64
	dc, err := NewUInt32GorillaDecompress(reader, &val)
	if err != nil {
		t.Fatal(err)
	}
	if val != uint64(math.Float32bits(ts[0])) {
		t.Fatalf("different first tick")
	}
	for i := 1; i < len(ts); i++ {
		err = dc.Decompress(reader, &val)
		if err != nil {
			t.Fatal(err)
		}
		if val != uint64(math.Float32bits(ts[i])) {
			t.Fatalf("different tick")
		}
	}
}
