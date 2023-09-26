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
	c := NewTickCompress(buf, ts1[0])
	for i := 1; i < len(ts1); i++ {
		c.Compress(buf, ts1[i])
	}
	c.Close(buf)
	reader := NewBitReader(buf)

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
		c.Compress(buf, ts2[i])
	}
	c.Close(buf)

	reader = NewBitReader(buf)
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
	N := 4000
	var tmp uint64 = 0
	ts := make([]uint64, N)
	for i := 0; i < N; i++ {
		tmp += uint64(rand.Int31n(10000))
		ts[i] = tmp
	}
	buf := NewBBuffer(nil, 0)
	c := NewTickCompress(buf, ts[0])
	for i := 1; i < len(ts); i++ {
		c.Compress(buf, ts[i])

		reader := NewBitReader(buf)

		dc, tick, err := NewTickDecompress(reader)
		if err != nil {
			t.Fatal(err)
		}
		if tick != ts[0] {
			t.Fatalf("different first tick")
		}
		for j := 1; j <= i; j++ {
			tick, err = dc.Decompress(reader)
			if err != nil {
				t.Fatal(err)
			}
			if tick != ts[j] {
				t.Fatalf("different tick")
			}
		}
	}
	c.Close(buf)

	fmt.Println(float64(len(buf.b)) / (8. * 40000000.))
}

func TestCompressChunkFuzz(t *testing.T) {
	N := 4000
	var tmp uint64 = 0
	ts := make([]uint64, N)
	for i := 0; i < N; i++ {
		tmp += uint64(rand.Int31n(10000))
		ts[i] = tmp
	}
	buf := NewBBuffer(nil, 0)
	reader := NewChunkReader(buf, 16)

	buf2 := NewBBuffer(nil, 0)
	writer := NewChunkWriter(buf2)
	c := NewTickCompress(buf, ts[0])
	for i := 1; i < len(ts); i++ {
		c.Compress(buf, ts[i])

		chunk := reader.ReadChunk()
		writer.WriteChunk(chunk)

		br := NewBitReader(buf2)
		dc, tick, err := NewTickDecompress(br)
		if err != nil {
			t.Fatal(err)
		}
		if tick != ts[0] {
			t.Fatalf("different first tick")
		}
		for j := 1; j <= i; j++ {
			tick, err = dc.Decompress(br)
			if err != nil {
				t.Fatal(err)
			}
			if tick != ts[j] {
				fmt.Println(buf.b, buf2.b)
				t.Fatalf("different tick")
			}
		}
	}
	c.Close(buf)

	fmt.Println(float64(len(buf.b)) / (8. * 40000000.))
}
