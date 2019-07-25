package gotickfile

import (
	"fmt"
	"os"
	"reflect"
	"testing"
)

type Data struct {
	Time uint64
	Price uint8
	Volume uint64
	Prob uint8
	Prib uint64
}

var data = Data{
	Time: 1299229200000,
	Price: 253,
	Volume: 8,
	Prob: 252,
	Prib: 2,
}


func TestCreate(t *testing.T) {
	tf, err := Create(
		"test.tick",
		WithDataType(reflect.TypeOf(Data{})),
		WithContentDescription("prices of acme at NYSE"),
		WithNameValues(map[string]interface{} {
			"decimals": int32(2),
			"url"     : "www.acme.com",
			"data"    : []byte{0x00, 0x01},
		}))
	if err != nil {
		t.Fatalf("error creating tickfile: %v", err)
	}

	err = tf.Write(0, data)
	if err != nil {
		t.Fatalf("error writing data to tickfile: %v", err)
	}

	err = tf.Write(1, data)
	if err != nil {
		t.Fatalf("error writing data to tickfile: %v", err)
	}

	err = tf.Write(2, data)
	if err != nil {
		t.Fatalf("error writing data to tickfile: %v", err)
	}

	err = tf.Write(3, data)
	if err != nil {
		t.Fatalf("error writing data to tickfile: %v", err)
	}

	err = tf.Write(14, data)
	if err != nil {
		t.Fatalf("error writing data to tickfile: %v", err)
	}

	err = tf.Write(30, data)
	if err != nil {
		t.Fatalf("error writing data to tickfile: %v", err)
	}

	err = tf.Close()
	if err != nil {
		t.Fatalf("error closing tickfile: %v", err)
	}

	tf, err = OpenRead("test.tick", reflect.TypeOf(Data{}))
	if err != nil {
		t.Fatalf("error opening tickfile: %v", err)
	}

	// Comparing with fixture
	goldTf, err := OpenRead("test-fixtures/test.tick", reflect.TypeOf(Data{}))
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

	if !reflect.DeepEqual(goldTf.ticks, tf.ticks) {
		t.Fatalf(
			"got different ticks: %v, %v",
			goldTf.ticks,
			tf.ticks)
	}

	err = os.Remove("test.tick")
	if err != nil {
		t.Fatalf("error deleting tickfile: %v", err)
	}
}

func TestBasicKind(t *testing.T) {
	var val float64 = 0.8
	tf, err := Create(
		"test.tick",
		WithBasicType(reflect.TypeOf(val)),
		WithContentDescription("prices of acme at NYSE"),
		WithNameValues(map[string]interface{} {
			"decimals": int32(2),
			"url"     : "www.acme.com",
			"data"    : []byte{0x00, 0x01},
		}))
	if err != nil {
		t.Fatalf("error creating tickfile: %v", err)
	}

	err = tf.Write(0, val)
	if err != nil {
		t.Fatalf("error writing data to tickfile: %v", err)
	}
	tick, res, err := tf.Read(0)
	resData := res.(*float64)
	if err != nil {
		t.Fatalf("error reading data from tickfile: %v", err)
	}
	if tick != 0 || *resData != 0.8 {
		t.Fatalf("got a different reading")
	}
}

func TestOpenWrite(t *testing.T) {
	tf, err := OpenWrite("test-fixtures/test.tick", reflect.TypeOf(Data{}))
	if err != nil {
		t.Fatalf("error opening tickfile: %v", err)
	}
	_, _, err = tf.Read(5)
	if err != nil {
		t.Fatalf("error reading data: %v", err)
	}
}

func TestCreate2(t *testing.T) {
	tf, err := Create(
		"test.tick",
		WithDataType(reflect.TypeOf(Data{})),
		WithContentDescription("prices of acme at NYSE"),
		WithNameValues(map[string]interface{} {
			"decimals": int32(2),
			"url"     : "www.acme.com",
			"data"    : []byte{0x00, 0x01},
		}))
	if err != nil {
		t.Fatalf("error creating tickfile: %v", err)
	}

	err = tf.Write(0, data)
	if err != nil {
		t.Fatalf("error writing data to tickfile: %v", err)
	}
	err = tf.Write(1, data)
	if err != nil {
		t.Fatalf("error writing data to tickfile: %v", err)
	}
	err = tf.Write(2, data)
	if err != nil {
		t.Fatalf("error writing data to tickfile: %v", err)
	}
	err = tf.Write(3, data)
	if err != nil {
		t.Fatalf("error writing data to tickfile: %v", err)
	}
	tick, res, err := tf.Read(0)
	if err != nil {
		t.Fatalf("error reading data: %v", err)
	}
	err = tf.Write(4, data)
	if err != nil {
		t.Fatalf("error writing data to tickfile: %v", err)
	}

	// try reading delta in the buffer
	tick, res, err = tf.Read(4)
	if err != nil {
		t.Fatalf("error reading data: %v", err)
	}
	tick, res, err = tf.Read(4)
	resData := res.(*Data)
	if tick != 4 || !reflect.DeepEqual(*resData, data) {
		t.Fatalf("got a different read than expected")
	}

	err = tf.Close()
	if err != nil {
		t.Fatalf("error closing file: %v", err)
	}

	// Now read

	tf, err = OpenRead("test.tick", reflect.TypeOf(Data{}))
	if err != nil {
		t.Fatalf("error opening tickfile: %v", err)
	}
	tick, res, err = tf.Read(4)
	resData = res.(*Data)
	if tick != 4 || !reflect.DeepEqual(*resData, data) {
		t.Fatalf("got a different read than expected")
	}

	err = os.Remove("test.tick")
	if err != nil {
		t.Fatalf("error deleting tickfile: %v", err)
	}
}

func TestRead(t *testing.T) {
	tf, err := OpenRead("test-fixtures/test.tick", reflect.TypeOf(Data{}))
	if err != nil {
		t.Fatalf("error opening tickfile: %v", err)
	}
	tick, res, err := tf.Read(0)
	if err != nil {
		t.Fatalf("error reading data: %v", err)
	}
	resData := res.(*Data)
	if tick != 0 || !reflect.DeepEqual(*resData, data) {
		fmt.Println(res, data)
		t.Fatalf("got a different read than expected")
	}
}

