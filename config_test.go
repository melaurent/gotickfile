package gotickfile

import (
	uuid "github.com/satori/go.uuid"
	"os"
	"reflect"
	"testing"
)

func TestWithDataType(t *testing.T) {
	type Data struct {
		Time uint64
		Price uint8
		Volume uint64
		Prob uint8
		Prib uint64
	}

	// Test that a config creates the correct item section
	fixture := ItemSection{
		Info: ItemSectionInfo{
			ItemSize: 40,
			ItemTypeName: "Data",
			FieldCount: 5,
		},
		Fields: []ItemSectionField{
			{Index: 0, Type: 8, Offset: 0, Name: "Time"},
			{Index: 1, Type: 5, Offset: 8, Name: "Price"},
			{Index: 2, Type: 8, Offset: 16, Name: "Volume"},
			{Index: 3, Type: 5, Offset: 24, Name: "Prob"},
			{Index: 4, Type: 8, Offset: 32, Name: "Prib"},
		},
	}
	tf, err := Create(
		"test.tick",
		WithDataType(reflect.TypeOf(Data{})))
	if err != nil {
		t.Fatalf("error creating TeaFile: %v", err)
	}
	if !reflect.DeepEqual(*tf.itemSection, fixture) {
		t.Fatalf("got different content description")
	}
	err = os.Remove("test.tick")
	if err != nil {
		t.Fatalf("error deleting TeaFile: %v", err)
	}
}

func TestWithContentDescription(t *testing.T) {
	// Test that a config creates the correct description
	fixture := ContentDescriptionSection{
		ContentDescription: "prices of acme at NYSE",
	}
	tf, err := Create(
		"test.tick",
		WithContentDescription("prices of acme at NYSE"))
	if err != nil {
		t.Fatalf("error creating TeaFile: %v", err)
	}
	if !reflect.DeepEqual(*tf.contentDescriptionSection, fixture) {
		t.Fatalf("got different content description")
	}
	err = os.Remove("test.tick")
	if err != nil {
		t.Fatalf("error deleting TeaFile: %v", err)
	}
}

func TestWithNameValues(t *testing.T) {
	// Test that a config creates the correct name value section
	id, _ := uuid.NewV1()
	fixture := NameValueSection{
		NameValues: map[string]interface{} {
			"a": int32(1),
			"b": "c",
			"c": float64(1.2),
			"d": id,
			"e": uint64(100),
		},
	}
	tf, err := Create(
		"test.tick",
		WithNameValues(map[string]interface{}{
			"a": int32(1),
			"b": "c",
			"c": float64(1.2),
			"d": id,
			"e": uint64(100),
		}))
	if err != nil {
		t.Fatalf("error creating TeaFile: %v", err)
	}
	if !reflect.DeepEqual(*tf.nameValueSection, fixture) {
		t.Fatalf("got different content description")
	}
	err = os.Remove("test.tick")
	if err != nil {
		t.Fatalf("error deleting TeaFile: %v", err)
	}
}