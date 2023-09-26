package gotickfile

import (
	"fmt"
	"reflect"
	"testing"
)

func TestTickBuffer_Write(t *testing.T) {
	var val float64 = 0.1
	b := NewTickBuffer(reflect.TypeOf(val), nil)

	var goldenVal []float64
	for i := 1; i < 100; i++ {
		val = 1.0 / float64(i)
		goldenVal = append(goldenVal, val)
	}
	if _, err := b.Write(&goldenVal); err != nil {
		t.Fatalf("error writing val: %v", err)
	}

	slice := *b.ToSlice().(*[]float64)
	for i := range goldenVal {
		if !reflect.DeepEqual(goldenVal[i], slice[i]) {
			t.Fatalf("different val")
		}
	}
}

func TestTickBuffer_WriteByteSlice(t *testing.T) {
	var val [10]byte
	b := NewTickBuffer(reflect.TypeOf(val), nil)

	var goldenVal [][10]byte
	for i := 1; i < 100; i++ {
		val[0] = byte(i)
		goldenVal = append(goldenVal, val)
	}
	if _, err := b.Write(&goldenVal); err != nil {
		t.Fatalf("error writing val: %v", err)
	}

	slice := *b.ToSlice().(*[][10]byte)
	for i := range goldenVal {
		fmt.Println(goldenVal[i], slice[i])
		if !reflect.DeepEqual(goldenVal[i], slice[i]) {
			t.Fatalf("different val")
		}
	}
}
