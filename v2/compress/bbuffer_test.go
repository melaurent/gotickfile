package compress

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
)

func TestNewChunkReader(t *testing.T) {
	buff := NewBBuffer(nil, 0)
	cr := NewChunkReader(buff, 4)
	buff2 := NewBBuffer(nil, 0)
	cw := NewChunkWriter(buff2)
	// Test, randomly write bits
	for i := 0; i < 1000; i++ {
		bit := rand.Int() % 2
		buff.WriteBit(bit == 0)
		// Check
		chunk := cr.ReadChunk()
		cw.WriteChunk(chunk)
		if !reflect.DeepEqual(buff.b, buff2.b) {
			fmt.Println(buff.b, buff2.b)
			t.Fatal("different buffers")
		}
		if buff.count != buff2.count {
			t.Fatal("different counts")
		}
	}

	buff = NewBBuffer(nil, 0)

	// Test, randomly write bits
	for i := 0; i < 10001; i++ {
		bit := rand.Int() % 2
		buff.WriteBit(bit == 0)

		cr = NewChunkReader(buff, 4)
		buff2 = NewBBuffer(nil, 0)
		cw = NewChunkWriter(buff2)
		// Check
		chunk := cr.ReadChunk()
		for chunk != nil {
			cw.WriteChunk(chunk)
			chunk = cr.ReadChunk()
		}
		if !reflect.DeepEqual(buff.b, buff2.b) {
			t.Fatal("different buffers")
		}
		if buff.count != buff2.count {
			t.Fatal("different counts")
		}
	}
}

func TestRewind(t *testing.T) {
	buff := NewBBuffer(nil, 0)
	buffRef := NewBBuffer(nil, 0)

	for i := 0; i < 1000; i++ {
		bit := rand.Int() % 2
		buff.WriteBit(bit == 0)
		buffRef.WriteBit(bit == 0)
		// Check
		// Random write and rewind
		wn := rand.Intn(20)
		for j := 0; j < wn; j++ {
			bit = rand.Int() % 2
			buff.WriteBit(bit == 0)
		}
		if err := buff.Rewind(wn); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(buff.b, buffRef.b) {
			t.Fatal("different buffers")
		}
		if buff.count != buffRef.count {
			t.Fatal("different counts")
		}
	}
}
