package gotickfile

import (
	"math/rand"
	"reflect"
	"testing"
)

func TestCompress(t *testing.T) {
	ts := []uint64{0, 10, 15, 20, 25, 30, 35, 40, 40, 40, 41}
	data := CompressTicks(ts)
	newTs, _ := DecompressTicks(data, len(ts))
	if !reflect.DeepEqual(ts, newTs) {
		t.Fatalf("different timestamps")
	}
}

func TestCompressFuzz(t *testing.T) {
	N := 100000
	var tmp uint64 = 0
	ts := make([]uint64, N)
	for i := 0; i < N; i++ {
		tmp += uint64(rand.Int31n(1000))
		ts[i] = tmp
	}
	data := CompressTicks(ts)
	newTs, _ := DecompressTicks(data, len(ts))
	if !reflect.DeepEqual(ts, newTs) {
		t.Fatalf("different timestamps")
	}
}
