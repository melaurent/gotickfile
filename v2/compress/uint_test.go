package compress

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
	"unsafe"
)

func TestUInt64Compress(t *testing.T) {
	ts1 := []uint64{0, 10, 15, 20, 25, 30, 35, 40, 40, 40, 41}
	buf := NewBBuffer(nil, 0)

	c := NewUInt64GorillaCompress(buf, ts1[0])
	for i := 1; i < len(ts1); i++ {
		v := ts1[i]
		c.Compress(buf, unsafe.Pointer(&v))
	}
	fmt.Println(len(buf.b))
	reader := NewBitReader(buf)

	var val uint64
	dc, err := NewUInt64GorillaDecompress(reader, unsafe.Pointer(&val))
	if err != nil {
		t.Fatal(err)
	}
	if val != ts1[0] {
		t.Fatalf("different tick")
	}
	for i := 1; i < len(ts1); i++ {
		err = dc.Decompress(reader, unsafe.Pointer(&val))
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
		v := ts2[i]
		c.Compress(buf, unsafe.Pointer(&v))
	}

	reader = NewBitReader(buf)
	dc, err = NewUInt64GorillaDecompress(reader, unsafe.Pointer(&val))
	if err != nil {
		t.Fatal(err)
	}
	if val != ts1[0] {
		t.Fatalf("different tick")
	}
	for i := 1; i < len(ts1); i++ {
		err = dc.Decompress(reader, unsafe.Pointer(&val))
		if err != nil {
			t.Fatal(err)
		}
		if val != ts1[i] {
			t.Fatalf("different tick %d %d", val, ts1[i])
		}
	}
	for i := 0; i < len(ts2); i++ {
		err = dc.Decompress(reader, unsafe.Pointer(&val))
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
	var tmp = 100.
	ts := make([]uint64, N)
	for i := 0; i < N; i++ {
		tmp += rand.Float64()
		ts[i] = uint64(tmp)
	}
	buf := NewBBuffer(nil, 0)
	start := time.Now()
	c := NewUInt64GorillaCompress(buf, ts[0])
	for i := 1; i < len(ts); i++ {
		v := ts[i]
		c.Compress(buf, unsafe.Pointer(&v))
	}
	fmt.Println(time.Since(start))

	fmt.Println(float64(len(buf.b)) / (8. * 40000000.))
	reader := NewBitReader(buf)

	var val uint64
	dc, err := NewUInt64GorillaDecompress(reader, unsafe.Pointer(&val))
	if err != nil {
		t.Fatal(err)
	}
	if val != ts[0] {
		t.Fatalf("different first tick")
	}
	start = time.Now()
	for i := 1; i < len(ts); i++ {
		err = dc.Decompress(reader, unsafe.Pointer(&val))
		if err != nil {
			t.Fatal(err)
		}
		if val != ts[i] {
			t.Fatalf("different tick")
		}
	}

	bytes := N * 8
	gigaBytes := float64(bytes) / float64(1e9)
	fmt.Println(time.Since(start), gigaBytes)
}
