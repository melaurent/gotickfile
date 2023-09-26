package gotickfile

import (
	"fmt"
	gotickfilev1 "github.com/melaurent/gotickfile"
	"github.com/melaurent/kafero"
	"io"
	"math/rand"
	"reflect"
	"sync"
	"testing"
	"unsafe"
)

type Data struct {
	Time   uint64
	Price  uint32
	Volume uint64
	Prob   uint32
	Prib   uint64
}

var data1 = Data{
	Time:   1299229200000,
	Price:  253,
	Volume: 8,
	Prob:   252,
	Prib:   2,
}

var data2 = Data{
	Time:   1299229200000,
	Price:  124,
	Volume: 8,
	Prob:   252,
	Prib:   2,
}

type RawTradeDelta struct {
	Part1       uint64
	RawQuantity uint64
	ID          uint64
	AggregateID uint64
}

var fs = kafero.NewMemMapFs()

var goldenFs = kafero.NewOsFs()

func TestBug2(t *testing.T) {
	file, err := fs.Create("test.tick")
	if err != nil {
		t.Fatalf("error creating file")
	}

	tf, err := Create(
		file,
		WithDataType(reflect.TypeOf(Data{})),
		WithContentDescription("prices of acme at NYSE"),
		WithNameValues(map[string]interface{}{
			"decimals": int32(2),
			"url":      "www.acme.com",
			"data":     []byte{0x00, 0x01},
		}))
	if err != nil {
		t.Fatalf("error creating tickfile: %v", err)
	}

	val1 := TickDeltas{
		Pointer: unsafe.Pointer(&data1),
		Len:     1,
	}
	val2 := TickDeltas{
		Pointer: unsafe.Pointer(&data2),
		Len:     1,
	}

	var ts uint64 = 10
	for i := 0; i < 10000; i++ {
		if err := tf.Flush(); err != nil {
			panic(err)
		}
		if i%20 == 0 {
			ts += uint64(rand.Intn(51000))
			data1.Prib = uint64(rand.Intn(12919))
			err = tf.Write(ts, val1)
			if err != nil {
				t.Fatalf("error writing data to tickfile: %v", err)
			}
		} else {
			err = tf.Write(ts, val2)
			if err != nil {
				t.Fatalf("error writing data to tickfile: %v", err)
			}
		}
		if err := tf.Flush(); err != nil {
			panic(err)
		}
		if err := tf.Flush(); err != nil {
			panic(err)
		}
		if err := tf.Flush(); err != nil {
			panic(err)
		}

		if err := tf.Close(); err != nil {
			panic(err)
		}
		tf, err = OpenWrite(file, reflect.TypeOf(Data{}))
		if err != nil {
			t.Fatalf("error opening tickfile: %v", err)
		}
		if tf.LastTick() != ts {
			t.Fatalf("different last tick: %d %d", tf.LastTick(), ts)
		}
	}
	if err := tf.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCreate(t *testing.T) {
	//handle := NewOSFileHandle("test.tick")
	//handle := NewMemFileHandle()
	file, err := fs.Create("test.tick")
	if err != nil {
		t.Fatalf("error creating file")
	}

	tf, err := Create(
		file,
		WithDataType(reflect.TypeOf(Data{})),
		WithContentDescription("prices of acme at NYSE"),
		WithNameValues(map[string]interface{}{
			"decimals": int32(2),
			"url":      "www.acme.com",
			"data":     []byte{0x00, 0x01},
		}))
	if err != nil {
		t.Fatalf("error creating tickfile: %v", err)
	}

	val1 := TickDeltas{
		Pointer: unsafe.Pointer(&data1),
		Len:     1,
	}
	val2 := TickDeltas{
		Pointer: unsafe.Pointer(&data2),
		Len:     1,
	}

	tss := []uint64{0, 1, 2, 3, 14, 30}
	for i, ts := range tss {
		if i%2 == 0 {
			err = tf.Write(ts, val1)
			if err != nil {
				t.Fatalf("error writing data to tickfile: %v", err)
			}
		} else {
			err = tf.Write(ts, val2)
			if err != nil {
				t.Fatalf("error writing data to tickfile: %v", err)
			}
		}
	}
	if err := tf.Close(); err != nil {
		t.Fatal(err)
	}
	file.Close()

	file, err = fs.Open("test.tick")
	if err != nil {
		t.Fatalf("error opening file: %v", err)
	}
	tf, err = OpenRead(file, reflect.TypeOf(Data{}))
	if err != nil {
		t.Fatalf("error opening tickfile: %v", err)
	}
	//fixtureHandle := NewOSFileHandle("test-fixtures/test.tick")
	// Comparing with fixture
	goldenFile, err := goldenFs.Open("test-fixtures/test.tick")
	if err != nil {
		t.Fatalf("error opening file: %v", err)
	}
	goldTf, err := OpenRead(goldenFile, reflect.TypeOf(Data{}))
	if err != nil {
		t.Fatalf("error opening golden tickfile: %v", err)
	}

	if !reflect.DeepEqual(goldTf.itemSection, tf.itemSection) {
		t.Fatalf(
			"got different item section: %v, %v",
			goldTf.itemSection,
			tf.itemSection)
	}

	if !reflect.DeepEqual(goldTf.nameValueSection, tf.nameValueSection) {
		t.Fatalf(
			"got different name value section: %v, %v",
			goldTf.nameValueSection,
			tf.nameValueSection)
	}

	if !reflect.DeepEqual(goldTf.contentDescriptionSection, tf.contentDescriptionSection) {
		t.Fatalf(
			"got different content description section: %v, %v",
			goldTf.contentDescriptionSection,
			tf.contentDescriptionSection)
	}

	reader, err := tf.GetTickReader()
	if err != nil {
		t.Fatal(err)
	}
	i := 0

	tick, deltas, err := reader.Next()
	for err == nil {
		if tick != tss[i] {
			t.Fatalf("got different tick: %d %d", tick, tss[i])
		}

		if i%2 == 0 {
			if *(*Data)(deltas.Pointer) != data1 {
				t.Fatalf("got different data: %v %v", *(*Data)(deltas.Pointer), data1)
			}
		} else {
			if *(*Data)(deltas.Pointer) != data2 {
				t.Fatalf("got different data: %v %v", *(*Data)(deltas.Pointer), data2)
			}
		}
		i += 1
		tick, deltas, err = reader.Next()
	}
	if err != io.EOF {
		t.Fatal(err)
	}

	if err = fs.Remove("test.tick"); err != nil {
		t.Fatalf("error deleting tickfile: %v", err)
	}
}

func TestBasicKind(t *testing.T) {
	var val float64 = 0.8
	file, err := fs.Create("test.tick")
	if err != nil {
		t.Fatalf("error creating file")
	}
	tf, err := Create(
		file,
		WithBasicType(reflect.TypeOf(val)),
		WithContentDescription("prices of acme at NYSE"),
		WithNameValues(map[string]interface{}{
			"decimals": int32(2),
			"url":      "www.acme.com",
			"data":     []byte{0x00, 0x01},
		}))
	if err != nil {
		t.Fatalf("error creating tickfile: %v", err)
	}

	delta := TickDeltas{
		Pointer: unsafe.Pointer(&val),
		Len:     1,
	}
	err = tf.Write(0, delta)
	if err != nil {
		t.Fatalf("error writing data to tickfile: %v", err)
	}
	if err := tf.Flush(); err != nil {
		t.Fatal(err)
	}
	reader, err := tf.GetTickReader()
	if err != nil {
		t.Fatal(err)
	}
	tick, deltas, err := reader.Next()
	if err != nil {
		t.Fatal(err)
	}
	if tick != 0 || *(*float64)(deltas.Pointer) != 0.8 {
		t.Fatalf("got a different reading")
	}
}

func TestOpenWrite(t *testing.T) {
	goldenFile, err := goldenFs.Open("test-fixtures/test.tick")
	if err != nil {
		t.Fatalf("error opening file: %v", err)
	}
	tf, err := OpenWrite(goldenFile, reflect.TypeOf(Data{}))
	if err != nil {
		t.Fatalf("error opening tickfile: %v", err)
	}

	reader, err := tf.GetTickReader()
	if err != nil {
		t.Fatal(err)
	}
	tick, deltas, err := reader.Next()
	if err != nil {
		t.Fatal(err)
	}
	if tick != 0 || *(*Data)(deltas.Pointer) != data1 {
		fmt.Println(tick, *(*Data)(deltas.Pointer), data1)
		t.Fatalf("got a different read than expected")
	}
}

func TestAppend(t *testing.T) {
	file, err := fs.Create("test.tick")
	if err != nil {
		t.Fatalf("error creating file")
	}

	tf, err := Create(
		file,
		WithDataType(reflect.TypeOf(Data{})),
		WithContentDescription("prices of acme at NYSE"),
		WithNameValues(map[string]interface{}{
			"decimals": int32(2),
			"url":      "www.acme.com",
			"data":     []byte{0x00, 0x01},
		}))
	if err != nil {
		t.Fatalf("error creating tickfile: %v", err)
	}

	delta1 := TickDeltas{
		Pointer: unsafe.Pointer(&data1),
		Len:     1,
	}
	for i := 0; i < 100; i++ {
		err = tf.Write(uint64(i), delta1)
		if err != nil {
			t.Fatalf("error writing data to tickfile: %v", err)
		}
	}
	if err := tf.Close(); err != nil {
		t.Fatalf("error closing tickfile: %v", err)
	}

	tf, err = OpenWrite(file, reflect.TypeOf(Data{}))
	if err != nil {
		t.Fatalf("error opening tickfile in write mode: %v", err)
	}

	delta2 := TickDeltas{
		Pointer: unsafe.Pointer(&data2),
		Len:     1,
	}
	for i := 100; i < 200; i++ {
		err = tf.Write(uint64(i), delta2)
		if err != nil {
			t.Fatalf("error writing data to tickfile: %v", err)
		}
	}
	err = tf.Close()
	if err != nil {
		t.Fatalf("error closing tickfile: %v", err)
	}
	tf, err = OpenWrite(file, reflect.TypeOf(Data{}))
	if err != nil {
		t.Fatalf("error opening tickfile in write mode: %v", err)
	}

	for i := 200; i < 300; i++ {
		err = tf.Write(uint64(i), delta1)
		if err != nil {
			t.Fatalf("error writing data to tickfile: %v", err)
		}
	}
	err = tf.Close()
	if err != nil {
		t.Fatalf("error closing tickfile: %v", err)
	}

	tf, err = OpenRead(file, reflect.TypeOf(Data{}))
	if err != nil {
		t.Fatalf("error opening tickfile for reading: %v", err)
	}

	reader, err := tf.GetTickReader()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		tick, deltas, err := reader.Next()
		if err != nil {
			t.Fatal(err)
		}
		if tick != uint64(i) {
			t.Fatalf("got different tick: %d %d", i, tick)
		}
		if *(*Data)(deltas.Pointer) != data1 {
			t.Fatalf("was expecting data1")
		}
	}
	for i := 100; i < 200; i++ {
		tick, deltas, err := reader.Next()
		if err != nil {
			t.Fatal(err)
		}
		if tick != uint64(i) {
			t.Fatalf("got different tick: %d %d", i, tick)
		}
		if *(*Data)(deltas.Pointer) != data2 {
			t.Fatalf("was expecting %v, got %v", data2, *(*Data)(deltas.Pointer))
		}
	}
	for i := 200; i < 300; i++ {
		tick, deltas, err := reader.Next()
		if err != nil {
			t.Fatal(err)
		}
		if tick != uint64(i) {
			t.Fatalf("got different tick: %d %d", i, tick)
		}
		if *(*Data)(deltas.Pointer) != data1 {
			t.Fatalf("was expecting %v, got %v", data2, *(*Data)(deltas.Pointer))
		}
	}
}

func TestCreate2(t *testing.T) {
	//handle := NewOSFileHandle("test.tick")
	file, err := fs.Create("test.tick")
	if err != nil {
		t.Fatalf("error creating file")
	}
	tf, err := Create(
		file,
		WithDataType(reflect.TypeOf(Data{})),
		WithContentDescription("prices of acme at NYSE"),
		WithNameValues(map[string]interface{}{
			"decimals": int32(2),
			"url":      "www.acme.com",
			"data":     []byte{0x00, 0x01},
		}))
	if err != nil {
		t.Fatalf("error creating tickfile: %v", err)
	}

	val := TickDeltas{
		Pointer: unsafe.Pointer(&data1),
		Len:     1,
	}
	err = tf.Write(0, val)
	if err != nil {
		t.Fatalf("error writing data to tickfile: %v", err)
	}
	if err := tf.Flush(); err != nil {
		t.Fatal(err)
	}
	reader, err := tf.GetTickReader()
	if err != nil {
		t.Fatal(err)
	}
	tick, deltas, err := reader.Next()
	if err != nil {
		t.Fatal(err)
	}
	if tick != 0 || *(*Data)(deltas.Pointer) != data1 {
		t.Fatalf("different read")
	}
	tick, deltas, err = reader.Next()
	if err != io.EOF {
		t.Fatalf("was expecting EOF")
	}
	err = tf.Write(1, val)
	if err != nil {
		t.Fatalf("error writing data to tickfile: %v", err)
	}
	if err := tf.Flush(); err != nil {
		t.Fatal(err)
	}
	tick, deltas, err = reader.Next()
	if err != nil {
		t.Fatal(err)
	}
	if tick != 1 || *(*Data)(deltas.Pointer) != data1 {
		t.Fatalf("different read")
	}
	_, _, err = reader.Next()
	if err != io.EOF {
		t.Fatalf("was expecting EOF")
	}

	err = tf.Write(2, val)
	if err != nil {
		t.Fatalf("error writing data to tickfile: %v", err)
	}
	if err := tf.Flush(); err != nil {
		t.Fatal(err)
	}
	tick, deltas, err = reader.Next()
	if err != nil {
		t.Fatal(err)
	}
	if tick != 2 || *(*Data)(deltas.Pointer) != data1 {
		t.Fatalf("different read")
	}
	_, _, err = reader.Next()
	if err != io.EOF {
		t.Fatalf("was expecting EOF")
	}

	err = tf.Write(3, val)
	if err != nil {
		t.Fatalf("error writing data to tickfile: %v", err)
	}
	if err := tf.Flush(); err != nil {
		t.Fatal(err)
	}
	tick, deltas, err = reader.Next()
	if err != nil {
		t.Fatal(err)
	}
	if tick != 3 || *(*Data)(deltas.Pointer) != data1 {
		t.Fatalf("different read")
	}
	_, _, err = reader.Next()
	if err != io.EOF {
		t.Fatalf("was expecting EOF")
	}

	err = tf.Write(4, val)
	if err != nil {
		t.Fatalf("error writing data to tickfile: %v", err)
	}
	if err := tf.Flush(); err != nil {
		t.Fatal(err)
	}
	tick, deltas, err = reader.Next()
	if err != nil {
		t.Fatal(err)
	}
	if tick != 4 || *(*Data)(deltas.Pointer) != data1 {
		t.Fatalf("different read")
	}
	_, _, err = reader.Next()
	if err != io.EOF {
		t.Fatalf("was expecting EOF")
	}

	err = tf.Close()
	if err != nil {
		t.Fatalf("error closing file: %v", err)
	}

	// Now read
	file, err = fs.Open("test.tick")
	if err != nil {
		t.Fatalf("error creating file")
	}
	tf, err = OpenRead(file, reflect.TypeOf(Data{}))
	if err != nil {
		t.Fatalf("error opening tickfile: %v", err)
	}
	// TODO ?
	/*
		tick, res, err = tf.ReadItem(4)
		resData = res.(*Data)
		if tick != 4 || !reflect.DeepEqual(*resData, data1) {
			t.Fatalf("got a different read than expected")
		}
	*/

	if err = fs.Remove("test.tick"); err != nil {
		t.Fatalf("error deleting tickfile: %v", err)
	}
}

func TestRead(t *testing.T) {
	//fixtureHandle := NewOSFileHandle("test-fixtures/test.tick")
	goldenFile, err := goldenFs.Open("test-fixtures/test.tick")
	if err != nil {
		t.Fatalf("error creating file")
	}
	tf, err := OpenRead(goldenFile, reflect.TypeOf(Data{}))
	if err != nil {
		t.Fatalf("error opening tickfile: %v", err)
	}
	reader, err := tf.GetTickReader()
	if err != nil {
		t.Fatal(err)
	}
	tss := []uint64{0, 1, 2, 3, 14, 30}
	for i := 0; i < len(tss); i++ {
		tick, _, err := reader.Next()
		if err != nil {
			t.Fatal(err)
		}
		if tick != tss[i] {
			t.Fatalf("different tick")
		}
	}
}

func TestReadWriteMode(t *testing.T) {
	//fixtureHandle := NewOSFileHandle("test-fixtures/test.tick")
	file, err := fs.Create("test.tick")
	if err != nil {
		t.Fatalf("error creating file")
	}
	tf, err := Create(
		file,
		WithDataType(reflect.TypeOf(Data{})),
		WithContentDescription("prices of acme at NYSE"),
		WithNameValues(map[string]interface{}{
			"decimals": int32(2),
			"url":      "www.acme.com",
			"data":     []byte{0x00, 0x01},
		}))
	if err != nil {
		t.Fatalf("error creating tickfile: %v", err)
	}

	var goldenDeltas []Data
	for i := 0; i < 300; i++ {
		delta := Data{
			Time:   uint64(i),
			Price:  10,
			Volume: 10,
			Prob:   0,
			Prib:   0,
		}
		val := TickDeltas{
			Pointer: unsafe.Pointer(&delta),
			Len:     1,
		}
		if err := tf.Write(uint64(i), val); err != nil {
			t.Fatalf("error writing: %v", err)
		}
		goldenDeltas = append(goldenDeltas, delta)
	}
	if err := tf.Flush(); err != nil {
		t.Fatal(err)
	}
	reader, err := tf.GetTickReader()
	if err != nil {
		t.Fatal(err)
	}
	for i := range goldenDeltas {
		tick, deltas, err := reader.Next()
		if err != nil {
			t.Fatal(err)
		}
		if tick != uint64(i) || *(*Data)(deltas.Pointer) != goldenDeltas[i] {
			t.Fatalf("got a different read than expected")
		}
	}
}

func TestReadSlice(t *testing.T) {
	//fixtureHandle := NewOSFileHandle("test-fixtures/test.tick")
	file, err := fs.Create("test.tick")
	if err != nil {
		t.Fatalf("error creating file")
	}
	tf, err := Create(
		file,
		WithDataType(reflect.TypeOf(Data{})),
		WithContentDescription("prices of acme at NYSE"),
		WithNameValues(map[string]interface{}{
			"decimals": int32(2),
			"url":      "www.acme.com",
			"data":     []byte{0x00, 0x01},
		}))
	if err != nil {
		t.Fatalf("error creating tickfile: %v", err)
	}

	var goldenDeltas []Data
	for i := 0; i < 100; i++ {
		delta := Data{
			Time:   uint64(i),
			Price:  10,
			Volume: 10,
			Prob:   128,
			Prib:   65,
		}
		val := TickDeltas{
			Pointer: unsafe.Pointer(&delta),
			Len:     1,
		}
		if err := tf.Write(uint64(i/10), val); err != nil {
			t.Fatalf("error writing: %v", err)
		}
		goldenDeltas = append(goldenDeltas, delta)
	}
	if err := tf.Flush(); err != nil {
		t.Fatal(err)
	}
	reader, err := tf.GetTickReader()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		tick, deltas, err := reader.Next()
		if err != nil {
			t.Fatal(err)
		}
		if tick != uint64(i) {
			t.Fatalf("got a different tick than expected: %d %d", tick, i)
		}
		ptr := deltas.Pointer
		for j := 0; j < 10; j++ {
			val := *(*Data)(ptr)
			if val != goldenDeltas[i*10+j] {
				t.Fatalf("got different delta %v %v", val, goldenDeltas[i*10+j])
			}
			ptr = unsafe.Pointer(uintptr(ptr) + reflect.TypeOf(Data{}).Size())
		}

	}
}

func TestFuzzWrite(t *testing.T) {
	file, err := fs.Create("test.tick")
	if err != nil {
		t.Fatalf("error creating file")
	}
	tf, err := Create(
		file,
		WithDataType(reflect.TypeOf(Data{})),
		WithContentDescription("prices of acme at NYSE"),
		WithNameValues(map[string]interface{}{
			"decimals": int32(2),
			"url":      "www.acme.com",
			"data":     []byte{0x00, 0x01},
		}))
	if err != nil {
		t.Fatalf("error creating tickfile: %v", err)
	}

	var goldenDeltas []Data
	for i := 0; i < 1000; i++ {
		delta := Data{
			Time:   uint64(i),
			Price:  uint32(rand.Int()),
			Volume: uint64(rand.Int()),
			Prob:   uint32(rand.Int()),
			Prib:   uint64(rand.Int()),
		}
		val := TickDeltas{
			Pointer: unsafe.Pointer(&delta),
			Len:     1,
		}
		j := 15
		for k := 0; k < j; k++ {
			if err := tf.Write(uint64(i), val); err != nil {
				t.Fatalf("error writing: %v", err)
			}
		}
		goldenDeltas = append(goldenDeltas, delta)
		// Randomly flush
		if i%5 == rand.Intn(5) {
			if err := tf.Flush(); err != nil {
				t.Fatal(err)
			}
		}
		// Randomly verify
		if i%5 == rand.Intn(5) {
			if err := tf.Flush(); err != nil {
				t.Fatal(err)
			}
			reader, err := tf.GetTickReader()
			if err != nil {
				t.Fatal(err)
			}
			for j := range goldenDeltas {
				tick, deltas, err := reader.Next()
				if err != nil {
					t.Fatal(err)
				}
				if tick != uint64(j) || *(*Data)(deltas.Pointer) != goldenDeltas[j] {
					t.Fatalf("got a different read than expected")
				}
			}
		}
		// Randomly close and open file
		if i%5 == rand.Intn(5) {
			if err := tf.Close(); err != nil {
				t.Fatal(err)
			}
			tf, err = OpenRead(file, reflect.TypeOf(Data{}))
			if err != nil {
				t.Fatal(err)
			}
			reader, err := tf.GetTickReader()
			if err != nil {
				t.Fatal(err)
			}
			for i := range goldenDeltas {
				tick, deltas, err := reader.Next()
				if err != nil {
					t.Fatal(err)
				}
				if tick != uint64(i) || *(*Data)(deltas.Pointer) != goldenDeltas[i] {
					fmt.Println(tick, uint64(i))
					t.Fatalf("got a different read than expected")
				}
			}
			if err := tf.Close(); err != nil {
				t.Fatal(err)
			}
			tf, err = OpenWrite(file, reflect.TypeOf(Data{}))
			if err != nil {
				t.Fatalf("error opening tickfile in write mode: %v", err)
			}
		}
	}
	if err := tf.Flush(); err != nil {
		t.Fatal(err)
	}
	reader, err := tf.GetTickReader()
	if err != nil {
		t.Fatal(err)
	}
	for i := range goldenDeltas {
		tick, deltas, err := reader.Next()
		if err != nil {
			t.Fatal(err)
		}
		if tick != uint64(i) || *(*Data)(deltas.Pointer) != goldenDeltas[i] {
			t.Fatalf("got a different read than expected")
		}
	}
}

func TestConcurrent(t *testing.T) {
	file, err := fs.Create("test.tick")
	if err != nil {
		t.Fatalf("error creating file")
	}
	tf, err := Create(
		file,
		WithDataType(reflect.TypeOf(Data{})),
		WithContentDescription("prices of acme at NYSE"),
		WithNameValues(map[string]interface{}{
			"decimals": int32(2),
			"url":      "www.acme.com",
			"data":     []byte{0x00, 0x01},
		}))
	if err != nil {
		t.Fatalf("error creating tickfile: %v", err)
	}

	for i := 0; i < 10; i++ {
		delta := Data{
			Time:   uint64(i),
			Price:  uint32(rand.Int()),
			Volume: uint64(rand.Int()),
			Prob:   uint32(rand.Int()),
			Prib:   uint64(rand.Int()),
		}
		val := TickDeltas{
			Pointer: unsafe.Pointer(&delta),
			Len:     1,
		}
		if err := tf.Write(uint64(i), val); err != nil {
			t.Fatalf("error writing: %v", err)
		}
	}
	tf.Flush()

	errChan := make(chan error, 100)

	var wg sync.WaitGroup
	wg.Add(4)
	N := 100000
	fn := func() {
		defer wg.Done()
		reader, err := tf.GetTickReader()
		if err != nil {
			t.Fatal(err)
		}
		var expectedTick uint64 = 0
		for expectedTick < uint64(N) {
			tick, _, err := reader.Next()
			if err != nil {
				if err == io.EOF {
					continue
				} else {
					errChan <- fmt.Errorf("unexpected ending: %v", err)
					return
				}
			}
			if tick != expectedTick {
				errChan <- fmt.Errorf("got different tick %d %d", tick, expectedTick)
				return
			}
			expectedTick += 1
		}
	}
	go fn()
	go fn()
	go fn()

	go func() {
		defer wg.Done()
		for i := 10; i < N; i++ {
			delta := Data{
				Time:   uint64(i),
				Price:  uint32(rand.Int()),
				Volume: uint64(rand.Int()),
				Prob:   uint32(rand.Int()),
				Prib:   uint64(rand.Int()),
			}
			val := TickDeltas{
				Pointer: unsafe.Pointer(&delta),
				Len:     1,
			}
			if err := tf.Write(uint64(i), val); err != nil {
				errChan <- fmt.Errorf("error writing: %v", err)
			}
			if err := tf.Flush(); err != nil {
				errChan <- err
			}
		}
	}()

	wg.Wait()

	select {
	case err := <-errChan:
		t.Fatal(err)
	default:

	}
}

func TestV1ToV2(t *testing.T) {
	v1, err := fs.Create("v1.tick")
	if err != nil {
		t.Fatalf("error creating file")
	}
	v2, err := fs.Create("v2.tick")
	if err != nil {
		t.Fatalf("error creating file")
	}
	tags1 := map[string]string{
		"tag1": "v1",
		"tag2": "v2",
	}
	nv1 := map[string]interface{}{
		"decimals": int32(2),
		"url":      "www.acme.com",
		"data":     []byte{0x00, 0x01},
	}
	desc1 := "prices of acme at NYSE"
	tfv1, err := gotickfilev1.Create(
		v1,
		gotickfilev1.WithDataType(reflect.TypeOf(Data{})),
		gotickfilev1.WithContentDescription(desc1),
		gotickfilev1.WithTags(tags1),
		gotickfilev1.WithNameValues(nv1))
	if err != nil {
		t.Fatalf("error creating tickfile: %v", err)
	}

	var goldenDeltas []Data
	for i := 0; i < 1000; i++ {
		delta := Data{
			Time:   uint64(i),
			Price:  uint32(rand.Int()),
			Volume: uint64(rand.Int()),
			Prob:   uint32(rand.Int()),
			Prib:   uint64(rand.Int()),
		}
		deltas := []Data{delta}
		if err := tfv1.Write(uint64(i), &deltas); err != nil {
			t.Fatalf("error writing: %v", err)
		}
		goldenDeltas = append(goldenDeltas, delta)
	}

	if err := tfv1.Close(); err != nil {
		t.Fatal(err)
	}

	if err := V1ToV2(v2, v1, reflect.TypeOf(Data{})); err != nil {
		t.Fatal(err)
	}

	tfv2, err := OpenRead(v2, reflect.TypeOf(Data{}))
	if err != nil {
		t.Fatal(err)
	}
	nv2 := tfv2.GetNameValues()
	if !reflect.DeepEqual(nv1, nv2) {
		t.Fatal("different name values")
	}
	tags2 := tfv2.GetTags()
	if !reflect.DeepEqual(tags1, tags2) {
		t.Fatal("different tags")
	}
	desc2 := tfv2.GetContentDescription()
	if desc2 == nil {
		t.Fatal("no description in v2")
	}
	if !reflect.DeepEqual(desc1, *desc2) {
		t.Fatal("different description")
	}

	reader, err := tfv2.GetTickReader()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < len(goldenDeltas); i++ {
		tick, deltas, err := reader.Next()
		if err != nil {
			t.Fatal(err)
		}
		if tick != uint64(i) || *(*Data)(deltas.Pointer) != goldenDeltas[i] {
			fmt.Println(*(*Data)(deltas.Pointer), goldenDeltas[i])
			t.Fatalf("got a different read than expected")
		}
	}
}

func BenchmarkWrite(b *testing.B) {
	b.StopTimer()
	file, err := fs.Create("test.tick")
	if err != nil {
		b.Fatalf("error creating file")
	}
	tf, err := Create(
		file,
		WithDataType(reflect.TypeOf(Data{})),
		WithContentDescription("prices of acme at NYSE"),
		WithNameValues(map[string]interface{}{
			"decimals": int32(2),
			"url":      "www.acme.com",
			"data":     []byte{0x00, 0x01},
		}))
	if err != nil {
		b.Fatalf("error creating tickfile: %v", err)
	}

	val := TickDeltas{
		Pointer: unsafe.Pointer(&data1),
		Len:     1,
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		if err := tf.Write(uint64(i), val); err != nil {
			b.Fatal(err)
		}
	}
	if err := tf.Flush(); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkRead(b *testing.B) {
	b.StopTimer()
	file, err := fs.Create("test.tick")
	if err != nil {
		b.Fatalf("error creating file")
	}
	tf, err := Create(
		file,
		WithDataType(reflect.TypeOf(Data{})),
		WithContentDescription("prices of acme at NYSE"),
		WithNameValues(map[string]interface{}{
			"decimals": int32(2),
			"url":      "www.acme.com",
			"data":     []byte{0x00, 0x01},
		}))
	if err != nil {
		b.Fatalf("error creating tickfile: %v", err)
	}

	val := TickDeltas{
		Pointer: unsafe.Pointer(&data1),
		Len:     1,
	}
	for i := 0; i < b.N; i++ {
		if err := tf.Write(uint64(i), val); err != nil {
			b.Fatal(err)
		}
	}
	if err := tf.Flush(); err != nil {
		b.Fatal(err)
	}
	reader, err := tf.GetTickReader()
	if err != nil {
		b.Fatal(err)
	}
	b.StartTimer()
	for ; err == nil; _, _, err = reader.Next() {
	}
}

func BenchmarkConcurrent(b *testing.B) {
	file, err := fs.Create("test.tick")
	if err != nil {
		b.Fatalf("error creating file")
	}
	tf, err := Create(
		file,
		WithDataType(reflect.TypeOf(Data{})),
		WithContentDescription("prices of acme at NYSE"),
		WithNameValues(map[string]interface{}{
			"decimals": int32(2),
			"url":      "www.acme.com",
			"data":     []byte{0x00, 0x01},
		}))
	if err != nil {
		b.Fatalf("error creating tickfile: %v", err)
	}

	errChan := make(chan error, 100)

	var wg sync.WaitGroup
	wg.Add(2)
	N := b.N

	go func() {
		defer wg.Done()
		for i := 0; i < N; i++ {
			delta := Data{
				Time:   uint64(i),
				Price:  uint32(rand.Int()),
				Volume: uint64(rand.Int()),
				Prob:   uint32(rand.Int()),
				Prib:   uint64(rand.Int()),
			}
			val := TickDeltas{
				Pointer: unsafe.Pointer(&delta),
				Len:     1,
			}
			if err := tf.Write(uint64(i), val); err != nil {
				errChan <- fmt.Errorf("error writing: %v", err)
			}
			if err := tf.Flush(); err != nil {
				errChan <- err
			}
		}
	}()

	go func() {
		defer wg.Done()
		reader, err := tf.GetTickReader()
		if err != nil {
			errChan <- err
			return
		}
		var expectedTick uint64 = 0
		for expectedTick < uint64(N) {
			tick, _, err := reader.Next()
			if err != nil {
				fmt.Println("UNEXPECTED ENDING", tick)
				errChan <- fmt.Errorf("unexpected ending: %v", err)
				return
			}
			if tick != expectedTick {
				errChan <- fmt.Errorf("got different tick %d %d", tick, expectedTick)
				return
			}
			expectedTick += 1
		}
	}()

	wg.Wait()

	select {
	case err := <-errChan:
		b.Fatal(err)
	default:

	}
}
