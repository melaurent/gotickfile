package compress

import (
	"fmt"
	"io"
	"math/rand"
	"testing"
)

func TestCompress(t *testing.T) {
	ts1 := []uint64{0, 10, 15, 20, 25, 30, 35, 40, 40, 40, 41}
	buf := NewBBuffer(nil, 0)
	c := NewTickCompress(ts1[0], buf)
	for i := 1; i < len(ts1); i++ {
		c.Compress(ts1[i], buf)
	}
	c.Close(buf)
	reader := NewBReader(buf)

	dc, tick, err := NewTickDecompress(reader)
	if err != nil {
		t.Fatal(err)
	}
	if tick != ts1[0] {
		t.Fatalf("different tick")
	}
	for i := 1; i < len(ts1); i++ {
		tick, err = dc.Decompress(reader)
		if err != nil {
			t.Fatal(err)
		}
		if tick != ts1[i] {
			t.Fatalf("different tick")
		}
	}
	_, err = dc.Decompress(reader)
	if err != io.EOF {
		t.Fatal(err)
	}

	ts2 := []uint64{42, 43, 45, 46}

	// Start writing again
	c = dc.ToCompress()
	// Open back the stream
	if err := c.Open(buf); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < len(ts2); i++ {
		c.Compress(ts2[i], buf)
	}
	c.Close(buf)

	reader = NewBReader(buf)
	dc, tick, err = NewTickDecompress(reader)
	if err != nil {
		t.Fatal(err)
	}
	if tick != ts1[0] {
		t.Fatalf("different tick")
	}
	for i := 1; i < len(ts1); i++ {
		tick, err = dc.Decompress(reader)
		if err != nil {
			t.Fatal(err)
		}
		if tick != ts1[i] {
			t.Fatalf("different tick %d %d", tick, ts1[i])
		}
	}
	for i := 0; i < len(ts2); i++ {
		tick, err = dc.Decompress(reader)
		if err != nil {
			t.Fatal(err)
		}
		if tick != ts2[i] {
			t.Fatalf("different tick")
		}
	}
	_, err = dc.Decompress(reader)
	if err != io.EOF {
		t.Fatal(err)
	}
}

func TestCompressFuzz(t *testing.T) {
	N := 40000000
	var tmp uint64 = 0
	ts := make([]uint64, N)
	for i := 0; i < N; i++ {
		tmp += uint64(rand.Int31n(1000))
		ts[i] = tmp
	}
	buf := NewBBuffer(nil, 0)
	c := NewTickCompress(ts[0], buf)
	for i := 1; i < len(ts); i++ {
		c.Compress(ts[i], buf)
	}
	c.Close(buf)

	fmt.Println(float64(len(buf.b)) / (8. * 40000000.))

	reader := NewBReader(buf)

	dc, tick, err := NewTickDecompress(reader)
	if err != nil {
		t.Fatal(err)
	}
	if tick != ts[0] {
		t.Fatalf("different first tick")
	}
	for i := 1; i < len(ts); i++ {
		tick, err = dc.Decompress(reader)
		if err != nil {
			t.Fatal(err)
		}
		if tick != ts[i] {
			t.Fatalf("different tick")
		}
	}
	_, err = dc.Decompress(reader)
	if err != io.EOF {
		t.Fatal(err)
	}
}
