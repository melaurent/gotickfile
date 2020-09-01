package gotickfile

import (
	"encoding/binary"
	"fmt"
	"github.com/satori/go.uuid"
	"io"
	"reflect"
)

type ItemSection struct {
	Info   ItemSectionInfo
	Fields []ItemSectionField
}

type ItemSectionInfo struct {
	ItemSize     uint32
	ItemTypeName string
	FieldCount   uint32
}

type ItemSectionField struct {
	Index  uint32
	Type   uint8
	Offset uint32
	Name   string
}

func (is *ItemSection) Read(r io.Reader, order binary.ByteOrder) error {
	err := binary.Read(r, order, &is.Info.ItemSize)
	if err != nil {
		return err
	}
	is.Info.ItemTypeName, err = readText(r, order)
	if err != nil {
		return err
	}
	err = binary.Read(r, order, &is.Info.FieldCount)
	if err != nil {
		return err
	}
	var i uint32
	for i = 0; i < is.Info.FieldCount; i++ {
		f := ItemSectionField{
			Index: i,
		}
		err = binary.Read(r, order, &f.Type)
		if err != nil {
			return err
		}
		err = binary.Read(r, order, &f.Offset)
		if err != nil {
			return err
		}
		f.Name, err = readText(r, order)
		if err != nil {
			return err
		}
		is.Fields = append(is.Fields, f)
	}
	return nil
}

func (is *ItemSection) Write(w io.Writer, order binary.ByteOrder) error {
	err := binary.Write(w, order, is.Info.ItemSize)
	if err != nil {
		return err
	}
	err = writeText(w, order, is.Info.ItemTypeName)
	if err != nil {
		return err
	}
	err = binary.Write(w, order, is.Info.FieldCount)
	if err != nil {
		return err
	}

	for _, field := range is.Fields {
		err = binary.Write(w, order, field.Type)
		if err != nil {
			return err
		}
		err = binary.Write(w, order, field.Offset)
		if err != nil {
			return err
		}
		err = writeText(w, order, field.Name)
		if err != nil {
			return err
		}
	}
	return nil
}

func (is *ItemSection) Size() int64 {
	var size int64 = 0
	// ItemSize
	size += 4
	// ItemTypeName
	size += textSize(is.Info.ItemTypeName)
	// FieldCount
	size += 4
	for _, field := range is.Fields {
		// FieldType
		size += 1
		// FieldOffset
		size += 4
		// FieldName
		size += textSize(field.Name)
	}
	return size
}

type NameValueSection struct {
	NameValues map[string]interface{}
}

func defaultNameValueSection() *NameValueSection {
	return &NameValueSection{
		NameValues: make(map[string]interface{}),
	}
}

func (nv *NameValueSection) Read(r io.Reader, order binary.ByteOrder) error {
	nv.NameValues = make(map[string]interface{})
	var count int32
	err := binary.Read(r, order, &count)
	if err != nil {
		return err
	}
	var i int32
	for i = 0; i < count; i += 1 {
		name, err := readText(r, order)
		if err != nil {
			return err
		}
		var kind int32
		err = binary.Read(r, order, &kind)
		if err != nil {
			return err
		}
		switch kind {
		case NAME_VALUE_INT32:
			var value int32
			err = binary.Read(r, order, &value)
			if err != nil {
				return err
			}
			nv.NameValues[name] = value

		case NAME_VALUE_TEXT:
			value, err := readText(r, order)
			if err != nil {
				return err
			}
			nv.NameValues[name] = value

		case NAME_VALUE_DOUBLE:
			var value float64
			err = binary.Read(r, order, &value)
			if err != nil {
				return err
			}
			nv.NameValues[name] = value

		case NAME_VALUE_UUID:
			bytes := make([]byte, 16)
			err = binary.Read(r, order, bytes)
			if err != nil {
				return err
			}
			value, err := uuid.FromBytes(bytes)
			if err != nil {
				return err
			}
			nv.NameValues[name] = value

		case NAME_VALUE_UINT64:
			var value uint64
			err = binary.Read(r, order, &value)
			if err != nil {
				return err
			}
			nv.NameValues[name] = value

		case NAME_VALUE_BYTES:
			var length int32
			err := binary.Read(r, order, &length)
			if err != nil {
				return err
			}
			value := make([]byte, length)
			err = binary.Read(r, order, value)
			if err != nil {
				return err
			}
			nv.NameValues[name] = value

		default:
			return fmt.Errorf("unknown name value kind %d", kind)
		}
	}
	return nil
}

func (nv *NameValueSection) Write(w io.Writer, order binary.ByteOrder) error {
	var count = int32(len(nv.NameValues))
	err := binary.Write(w, order, count)
	if err != nil {
		return err
	}
	for key, val := range nv.NameValues {
		err = writeText(w, order, key)
		if err != nil {
			return err
		}
		nameValueType := typeToNameValueType[reflect.TypeOf(val).String()]
		err := binary.Write(w, order, nameValueType)
		if err != nil {
			return err
		}
		switch nameValueType {

		case NAME_VALUE_TEXT:
			err = writeText(w, order, val.(string))

		case NAME_VALUE_BYTES:
			err = writeBytes(w, order, val.([]byte))

		default:
			err = binary.Write(w, order, val)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (nv *NameValueSection) Size() int64 {
	var size int64 = 0
	// Count
	size += 4
	for name, val := range nv.NameValues {
		// Name
		size += textSize(name)
		// ValueType
		size += 4
		// Value
		nameValueType := typeToNameValueType[reflect.TypeOf(val).String()]

		switch nameValueType {
		case NAME_VALUE_TEXT:
			size += textSize(val.(string))

		case NAME_VALUE_BYTES:
			size += 4
			size += int64(len(val.([]byte)))

		default:
			size += int64(reflect.TypeOf(val).Size())
		}
	}
	return size
}

type TagsSection struct {
	Tags map[string]string
}

func defaultTagsSection() *TagsSection {
	return &TagsSection{
		Tags: make(map[string]string),
	}
}

func (t *TagsSection) Read(r io.Reader, order binary.ByteOrder) error {
	t.Tags = make(map[string]string)
	var count int32
	err := binary.Read(r, order, &count)
	if err != nil {
		return err
	}
	var i int32
	for i = 0; i < count; i += 1 {
		name, err := readText(r, order)
		if err != nil {
			return err
		}
		value, err := readText(r, order)
		if err != nil {
			return err
		}
		t.Tags[name] = value
	}
	return nil
}

func (t *TagsSection) Write(w io.Writer, order binary.ByteOrder) error {
	var count = int32(len(t.Tags))
	if err := binary.Write(w, order, count); err != nil {
		return err
	}
	for key, val := range t.Tags {
		if err := writeText(w, order, key); err != nil {
			return err
		}
		if err := writeText(w, order, val); err != nil {
			return err
		}
	}
	return nil
}

func (nv *TagsSection) Size() int64 {
	var size int64 = 0
	// Count
	size += 4
	for name, val := range nv.Tags {
		// Name
		size += textSize(name)
		// Value
		size += textSize(val)
	}
	return size
}

/*
type TimeSection struct {
	Epoch       uint64
	StartEpoch  uint64
	EndEpoch    uint64
	TicksPerDay uint64
}

// default to micro seconds
func defaultTimeSection() *TimeSection {
	return &TimeSection{
		Epoch: 719162,
		TicksPerDay: 86400000000,
	}
}

func (ts *TimeSection) Read(r io.Reader, order binary.ByteOrder) error {
	err := binary.Read(r, order, &ts.Epoch)
	if err != nil { return err }
	err = binary.Read(r, order, &ts.TicksPerDay)
	if err != nil { return err }
	err = binary.Read(r, order, &ts.StartEpoch)
	if err != nil { return err }
	err = binary.Read(r, order, &ts.EndEpoch)
	if err != nil { return err }
	return nil
}

func (ts *TimeSection) Write(w io.Writer, order binary.ByteOrder) error {
	err := binary.Write(w, order, ts.Epoch)
	if err != nil { return err }
	err = binary.Write(w, order, ts.TicksPerDay)
	if err != nil { return err }
	err = binary.Write(w, order, ts.StartEpoch)
	if err != nil { return err }
	err = binary.Write(w, order, ts.EndEpoch)
	if err != nil { return err }
	return nil
}

func (ts *TimeSection) Size() int64 {
	var size int64 = 0
	// Epoch
	size += 8
	// StartEpoch
	size += 8
	// EndEpoch
	size += 8
	// TicksPerDay
	size += 8

	return size
}
*/

type ContentDescriptionSection struct {
	ContentDescription string
}

// default to micro second with no time field
func defaultContentDescriptionSection() *ContentDescriptionSection {
	return &ContentDescriptionSection{
		ContentDescription: "",
	}
}

func (s *ContentDescriptionSection) Read(r io.Reader, order binary.ByteOrder) error {
	var err error
	s.ContentDescription, err = readText(r, order)
	return err
}

func (s *ContentDescriptionSection) Write(r io.Writer, order binary.ByteOrder) error {
	return writeText(r, order, s.ContentDescription)
}

func (s *ContentDescriptionSection) Size() int64 {
	var size int64 = 0
	size += textSize(s.ContentDescription)

	return size
}
