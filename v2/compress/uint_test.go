package compress

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestUInt64Compress(t *testing.T) {
	ts1 := []uint64{0, 10, 15, 20, 25, 30, 35, 40, 40, 40, 41}
	buf := NewBBuffer(nil, 0)

	c := NewUInt64GorillaCompress(ts1[0], buf)
	for i := 1; i < len(ts1); i++ {
		c.Compress(ts1[i], buf)
	}
	fmt.Println(len(buf.b))
	reader := NewBitReader(buf)

	var val uint64
	dc, err := NewUInt64GorillaDecompress(reader, &val)
	if err != nil {
		t.Fatal(err)
	}
	if val != ts1[0] {
		t.Fatalf("different tick")
	}
	for i := 1; i < len(ts1); i++ {
		err = dc.Decompress(reader, &val)
		if err != nil {
			t.Fatal(err)
		}
		if val != ts1[i] {
			t.Fatalf("different tick")
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
	if val != ts1[0] {
		t.Fatalf("different tick")
	}
	for i := 1; i < len(ts1); i++ {
		err = dc.Decompress(reader, &val)
		if err != nil {
			t.Fatal(err)
		}
		if val != ts1[i] {
			t.Fatalf("different tick %d %d", val, ts1[i])
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

func TestUInt64CompressFuzz(t *testing.T) {
	N := 40000000
	var tmp uint64 = 100000
	ts := make([]uint64, N)
	for i := 0; i < N; i++ {
		tmp += uint64(rand.Int31n(10))
		ts[i] = tmp
	}
	buf := NewBBuffer(nil, 0)
	c := NewUInt64GorillaCompress(ts[0], buf)
	for i := 1; i < len(ts); i++ {
		c.Compress(ts[i], buf)
	}

	fmt.Println(float64(len(buf.b)) / (8. * 40000000.))
	reader := NewBitReader(buf)

	var val uint64
	dc, err := NewUInt64GorillaDecompress(reader, &val)
	if err != nil {
		t.Fatal(err)
	}
	if val != ts[0] {
		t.Fatalf("different first tick")
	}
	for i := 1; i < len(ts); i++ {
		err = dc.Decompress(reader, &val)
		if err != nil {
			t.Fatal(err)
		}
		if val != ts[i] {
			t.Fatalf("different tick")
		}
	}
}
