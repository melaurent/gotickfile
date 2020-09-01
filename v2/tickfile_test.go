package gotickfile

import (
	"github.com/melaurent/kafero"
	"io"
	"math/rand"
	"reflect"
	"testing"
)

type Data struct {
	Time   uint64
	Price  uint8
	Volume uint64
	Prob   uint8
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

var fs = kafero.NewMemMapFs()

var goldenFs = kafero.NewOsFs()

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

	val1 := reflect.ValueOf(&data1)
	val2 := reflect.ValueOf(&data2)

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

	reader, err := tf.GetReader()
	if err != nil {
		t.Fatal(err)
	}
	i := 0
	for ; err == nil; err = reader.Next() {
		if reader.Tick != tss[i] {
			t.Fatalf("got different tick: %d %d", reader.Tick, tss[i])
		}

		if i%2 == 0 {
			if *reader.Val.(*Data) != data1 {
				t.Fatalf("got different data: %v %v", *reader.Val.(*Data), data1)
			}
		} else {
			if *reader.Val.(*Data) != data2 {
				t.Fatalf("got different data: %v %v", *reader.Val.(*Data), data2)
			}
		}
		i += 1
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

	delta := reflect.ValueOf(&val)
	err = tf.Write(0, delta)
	if err != nil {
		t.Fatalf("error writing data to tickfile: %v", err)
	}
	if err := tf.Flush(); err != nil {
		t.Fatal(err)
	}
	reader, err := tf.GetReader()
	if err != nil {
		t.Fatal(err)
	}
	if reader.Tick != 0 || *reader.Val.(*float64) != 0.8 {
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

	reader, err := tf.GetReader()
	if err != nil {
		t.Fatal(err)
	}

	if reader.Tick != 0 || *reader.Val.(*Data) != data1 {
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

	delta1 := reflect.ValueOf(&data1)
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

	delta2 := reflect.ValueOf(&data2)
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

	reader, err := tf.GetReader()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		if reader.Tick != uint64(i) {
			t.Fatalf("got different tick: %d %d", i, reader.Tick)
		}
		tmp := reader.Val.(*Data)
		if *tmp != data1 {
			t.Fatalf("was expecting data1")
		}
		if err := reader.Next(); err != nil {
			t.Fatal(err)
		}
	}
	for i := 100; i < 200; i++ {
		if reader.Tick != uint64(i) {
			t.Fatalf("got different tick: %d %d", i, reader.Tick)
		}
		tmp := reader.Val.(*Data)
		if *tmp != data2 {
			t.Fatalf("was expecting %v, got %v", data2, *tmp)
		}
		if err := reader.Next(); err != nil {
			t.Fatal(err)
		}
	}
	for i := 200; i < 300; i++ {
		if reader.Tick != uint64(i) {
			t.Fatalf("got different tick: %d %d", i, reader.Tick)
		}
		tmp := reader.Val.(*Data)
		if *tmp != data1 {
			t.Fatalf("was expecting %v, got %v", data2, *tmp)
		}
		if i < 299 {
			if err := reader.Next(); err != nil {
				t.Fatal(err)
			}
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

	val := reflect.ValueOf(&data1)

	err = tf.Write(0, val)
	if err != nil {
		t.Fatalf("error writing data to tickfile: %v", err)
	}
	if err := tf.Flush(); err != nil {
		t.Fatal(err)
	}
	reader, err := tf.GetReader()
	if err != nil {
		t.Fatal(err)
	}
	if reader.Tick != 0 || *reader.Val.(*Data) != data1 {
		t.Fatalf("different read")
	}
	if err := reader.Next(); err != io.EOF {
		t.Fatalf("was expecting EOF")
	}
	err = tf.Write(1, val)
	if err != nil {
		t.Fatalf("error writing data to tickfile: %v", err)
	}
	if err := tf.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := reader.Next(); err != nil {
		t.Fatal(err)
	}
	if reader.Tick != 1 || *reader.Val.(*Data) != data1 {
		t.Fatalf("different read")
	}
	if err := reader.Next(); err != io.EOF {
		t.Fatalf("was expecting EOF")
	}

	err = tf.Write(2, val)
	if err != nil {
		t.Fatalf("error writing data to tickfile: %v", err)
	}
	if err := tf.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := reader.Next(); err != nil {
		t.Fatal(err)
	}
	if reader.Tick != 2 || *reader.Val.(*Data) != data1 {
		t.Fatalf("different read")
	}
	if err := reader.Next(); err != io.EOF {
		t.Fatalf("was expecting EOF")
	}

	err = tf.Write(3, val)
	if err != nil {
		t.Fatalf("error writing data to tickfile: %v", err)
	}
	if err := tf.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := reader.Next(); err != nil {
		t.Fatal(err)
	}
	if reader.Tick != 3 || *reader.Val.(*Data) != data1 {
		t.Fatalf("different read")
	}
	if err := reader.Next(); err != io.EOF {
		t.Fatalf("was expecting EOF")
	}

	err = tf.Write(4, val)
	if err != nil {
		t.Fatalf("error writing data to tickfile: %v", err)
	}
	if err := tf.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := reader.Next(); err != nil {
		t.Fatal(err)
	}
	if reader.Tick != 4 || *reader.Val.(*Data) != data1 {
		t.Fatalf("different read")
	}
	if err := reader.Next(); err != io.EOF {
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
	reader, err := tf.GetReader()
	if err != nil {
		t.Fatal(err)
	}
	tss := []uint64{0, 1, 2, 3, 14, 30}
	i := 0
	for ; err == nil; err = reader.Next() {
		if reader.Tick != tss[i] {
			t.Fatalf("different tick")
		}
		i += 1
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
		val := reflect.ValueOf(&delta)
		if err := tf.Write(0, val); err != nil {
			t.Fatalf("error writing: %v", err)
		}
		goldenDeltas = append(goldenDeltas, delta)
	}
	if err := tf.Flush(); err != nil {
		t.Fatal(err)
	}
	reader, err := tf.GetReader()
	if err != nil {
		t.Fatal(err)
	}
	for i := range goldenDeltas {
		if reader.Tick != 0 || *reader.Val.(*Data) != goldenDeltas[i] {
			t.Fatalf("got a different read than expected")
		}
		if err := reader.Next(); err != nil {
			if err == io.EOF && i == len(goldenDeltas)-1 {
				continue
			} else {
				t.Fatal(err)
			}
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
			Price:  uint8(rand.Int()),
			Volume: uint64(rand.Int()),
			Prob:   uint8(rand.Int()),
			Prib:   uint64(rand.Int()),
		}
		val := reflect.ValueOf(&delta)
		if err := tf.Write(uint64(i), val); err != nil {
			t.Fatalf("error writing: %v", err)
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
			reader, err := tf.GetReader()
			if err != nil {
				t.Fatal(err)
			}
			for i := range goldenDeltas {
				if reader.Tick != uint64(i) || *reader.Val.(*Data) != goldenDeltas[i] {
					t.Fatalf("got a different read than expected")
				}
				if err := reader.Next(); err != nil {
					if err == io.EOF && i == len(goldenDeltas)-1 {
						continue
					} else {
						t.Fatal(err)
					}
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
			reader, err := tf.GetReader()
			if err != nil {
				t.Fatal(err)
			}
			for i := range goldenDeltas {
				if reader.Tick != uint64(i) || *reader.Val.(*Data) != goldenDeltas[i] {
					t.Fatalf("got a different read than expected")
				}
				if err := reader.Next(); err != nil {
					if err == io.EOF && i == len(goldenDeltas)-1 {
						continue
					} else {
						t.Fatal(err)
					}
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
	reader, err := tf.GetReader()
	if err != nil {
		t.Fatal(err)
	}
	for i := range goldenDeltas {
		if reader.Tick != uint64(i) || *reader.Val.(*Data) != goldenDeltas[i] {
			t.Fatalf("got a different read than expected")
		}
		if err := reader.Next(); err != nil {
			if err == io.EOF && i == len(goldenDeltas)-1 {
				continue
			} else {
				t.Fatal(err)
			}
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

	val := reflect.ValueOf(&data1)
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

	val := reflect.ValueOf(&data1)
	for i := 0; i < b.N; i++ {
		if err := tf.Write(uint64(i), val); err != nil {
			b.Fatal(err)
		}
	}
	if err := tf.Flush(); err != nil {
		b.Fatal(err)
	}
	reader, err := tf.GetReader()
	if err != nil {
		b.Fatal(err)
	}
	b.StartTimer()
	for ; err == nil; err = reader.Next() {
	}
}
