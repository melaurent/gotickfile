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
