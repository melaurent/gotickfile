package gotickfile

import (
	"encoding/binary"
	"fmt"
	"github.com/melaurent/gotickfile/v2/compress"
	"github.com/melaurent/kafero"
	"io"
	"io/ioutil"
	"reflect"
	"unsafe"
)

// Should tickfile have a file handle ?
// Because we can map file to memory, or if we open in readonly mode
// we can give a reader over the in memory buffer instead.
// the problem is that the reader will do a copy.
// we want to avoid copy. So tickfile should have direct access
// to the file content so it can return pointer to delta without copy
// So file handle and file system

var nativeEndian binary.ByteOrder

func init() {
	buf := [2]byte{}
	*(*uint16)(unsafe.Pointer(&buf[0])) = uint16(0xABCD)

	switch buf {
	case [2]byte{0xCD, 0xAB}:
		nativeEndian = binary.LittleEndian
	case [2]byte{0xAB, 0xCD}:
		nativeEndian = binary.BigEndian
	default:
		panic("could not determine native endianness.")
	}
}

type TickDeltas struct {
	Pointer unsafe.Pointer
	Len     int
}

type Header struct {
	MagicValue   int64
	ItemStart    int64
	SectionCount int64
}

type TickFile struct {
	file                      kafero.File
	offset                    int64
	write                     bool
	writer                    *CTickWriter
	lastTick                  uint64
	block                     *compress.BBuffer
	buffer                    *compress.BBuffer
	dataType                  reflect.Type
	header                    Header
	itemSection               *ItemSection
	nameValueSection          *NameValueSection
	tagsSection               *TagsSection
	contentDescriptionSection *ContentDescriptionSection
	tmpVal                    reflect.Value
}

func Create(file kafero.File, configs ...TickFileConfig) (*TickFile, error) {
	var tf *TickFile

	if err := file.Truncate(0); err != nil {
		return nil, fmt.Errorf("error truncating file: %v", err)
	}

	tf = &TickFile{
		file:   file,
		write:  true,
		writer: nil,
	}

	for _, config := range configs {
		config(tf)
	}
	if tf.dataType == nil {
		return nil, fmt.Errorf("no data type")
	}
	tf.header.SectionCount = 0
	tf.header.ItemStart = int64(reflect.TypeOf(tf.header).Size())
	tf.header.MagicValue = 0x0d0e0a0402080502

	if tf.itemSection != nil {
		err := tf.checkDataType()
		if err != nil {
			return nil, err
		}

		tf.header.SectionCount += 1
		// Section ID
		tf.header.ItemStart += 4
		// Next Section Offset
		tf.header.ItemStart += 4
		// Item Section
		tf.header.ItemStart += tf.itemSection.Size()
	}

	if tf.nameValueSection != nil {
		tf.header.SectionCount += 1
		// Section ID
		tf.header.ItemStart += 4
		// Next Section Offset
		tf.header.ItemStart += 4
		// Name Value Section
		tf.header.ItemStart += tf.nameValueSection.Size()
	}

	if tf.tagsSection != nil {
		tf.header.SectionCount += 1
		// Section ID
		tf.header.ItemStart += 4
		// Next Section Offset
		tf.header.ItemStart += 4
		// Name Value Section
		tf.header.ItemStart += tf.tagsSection.Size()
	}

	if tf.contentDescriptionSection != nil {
		tf.header.SectionCount += 1
		// Section ID
		tf.header.ItemStart += 4
		// Next Section Offset
		tf.header.ItemStart += 4
		// Content Description Section
		tf.header.ItemStart += tf.contentDescriptionSection.Size()
	}

	// Align ItemStart on 8 bytes
	paddingBytes := 8 - tf.header.ItemStart%8

	tf.header.ItemStart += paddingBytes

	if err := tf.writeHeader(); err != nil {
		return nil, err
	}

	tf.tmpVal = reflect.New(tf.dataType)
	tf.lastTick = 0
	tf.block = compress.NewBBuffer(nil, 0)
	tf.buffer = compress.NewBBuffer(nil, 0)

	if _, err := tf.file.Seek(tf.header.ItemStart, io.SeekStart); err != nil {
		return nil, err
	}
	tf.offset = tf.header.ItemStart

	return tf, nil
}

func (tf *TickFile) GetNameValues() map[string]interface{} {
	if tf.nameValueSection != nil {
		return tf.nameValueSection.NameValues
	} else {
		return nil
	}
}

func (tf *TickFile) GetTags() map[string]string {
	if tf.tagsSection != nil {
		return tf.tagsSection.Tags
	} else {
		return nil
	}
}

func (tf *TickFile) GetFile() kafero.File {
	return tf.file
}

func OpenWrite(file kafero.File, dataType reflect.Type) (*TickFile, error) {
	tf := &TickFile{
		file:     file,
		write:    true,
		writer:   nil,
		dataType: dataType,
	}

	if _, err := tf.file.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("error seeking to beginning of file: %v", err)
	}

	if err := tf.readHeader(); err != nil {
		return nil, err
	}

	if err := tf.checkDataType(); err != nil {
		return nil, err
	}

	if _, err := tf.file.Seek(tf.header.ItemStart, 0); err != nil {
		return nil, err
	}
	tf.offset = tf.header.ItemStart

	// Read file to block
	block, err := ioutil.ReadAll(tf.file)
	if err != nil {
		return nil, err
	}
	tf.offset += int64(len(block))

	if len(block) == 0 {
		tf.block = compress.NewBBuffer(nil, 0)
		tf.buffer = compress.NewBBuffer(nil, 0)
	} else {
		// Create buffer from block
		tf.block, err = blockToBuffer(block)
		if err != nil {
			return nil, err
		}
		tf.buffer = tf.block.CloneTip(2)
		// Read to the end
		w, err := CTickWriterFromBlock(tf.itemSection, tf.dataType, tf.block)
		if err != nil {
			return nil, fmt.Errorf("error loading writer from block: %v", err)
		}
		// Open buffer
		if err := w.Open(tf.buffer); err != nil {
			return nil, fmt.Errorf("error opening buffer for writing: %v", err)
		}
		tf.writer = w
	}

	tf.tmpVal = reflect.New(tf.dataType)
	return tf, nil
}

func blockToBuffer(block []byte) (*compress.BBuffer, error) {
	// Look for EOF value in block
	// look in the last byte
	// XXX11111 -> count = 0
	// XX111110 -> count = 1
	// X1111100 -> count = 2
	// 11111000 -> count = 3
	// XXXXXXX1 11110000 -> count = 4
	// XXXXXX11 11100000 -> count = 5
	// XXXXX111 11000000 -> count = 6
	// XXXX1111 10000000 -> count = 7
	i := len(block) - 1
	var v uint64
	if i > 0 {
		v = uint64(block[i-1])<<8 | uint64(block[i])
	} else {
		v = uint64(block[i])
	}
	var count uint8 = 0
	for v != 0 && v&0x1F != 0x1F {
		v >>= 1
		count += 1
	}
	if v == 0 {
		return nil, fmt.Errorf("no EOF found in block")
	} else {
		return compress.NewBBuffer(block, count), nil
	}
}

// ticks: ticks since epoch
func (tf *TickFile) Write(tick uint64, val TickDeltas) error {
	if !tf.write {
		return ErrReadOnly
	}

	if tf.itemSection == nil {
		return fmt.Errorf("this file has no item section")
	}

	/*
		expectedType := reflect.PtrTo(reflect.SliceOf(tf.dataType))

		if reflect.TypeOf(val) != expectedType {
			return fmt.Errorf("was expecting pointer to slice of %s, got %s", tf.dataType, reflect.TypeOf(val))
		}
	*/

	if tick < tf.lastTick {
		return ErrTickOutOfOrder
	}

	size := tf.dataType.Size()
	count := val.Len
	ptr := uintptr(val.Pointer)
	if tf.writer == nil {
		tf.writer = NewCTickWriter(tf.itemSection, tick, ptr, tf.buffer)
		ptr += size
		count -= 1
	}

	for i := 0; i < count; i++ {
		tf.writer.Write(tick, ptr, tf.buffer)
		ptr += size
	}
	tf.lastTick = tick

	// TODO flush
	return nil
}

func OpenRead(file kafero.File, dataType reflect.Type) (*TickFile, error) {
	tf := &TickFile{
		file:     file,
		write:    false,
		dataType: dataType,
	}

	if _, err := tf.file.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("error seeking to beginning of file: %v", err)
	}

	if err := tf.readHeader(); err != nil {
		return nil, fmt.Errorf("error reading file header: %v", err)
	}

	if err := tf.checkDataType(); err != nil {
		return nil, fmt.Errorf("error checking data type: %v", err)
	}

	if _, err := tf.file.Seek(tf.header.ItemStart, 0); err != nil {
		return nil, fmt.Errorf("error seeking to first item: %v", err)
	}
	tf.offset = tf.header.ItemStart
	// Read file to block
	block, err := ioutil.ReadAll(tf.file)
	if err != nil {
		return nil, err
	}
	tf.offset += int64(len(block))

	if len(block) == 0 {
		tf.block = compress.NewBBuffer(nil, 0)
	} else {
		// Create buffer from block
		tf.block, err = blockToBuffer(block)
		if err != nil {
			return nil, err
		}
	}

	tf.tmpVal = reflect.New(tf.dataType)
	return tf, nil
}

func OpenHeader(file kafero.File) (*TickFile, error) {

	tf := &TickFile{
		file: file,
	}

	if _, err := tf.file.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("error seeking to beginning of file: %v", err)
	}

	if err := tf.readHeader(); err != nil {
		return nil, err
	}

	return tf, nil
}

func (tf *TickFile) GetReader() (*CTickReader, error) {
	if len(tf.block.Bytes()) == 0 {
		return nil, io.EOF
	} else {
		return NewCTickReader(tf.itemSection, tf.dataType, compress.NewBReader(tf.block))
	}
}

func (tf *TickFile) Flush() error {
	if tf.writer == nil {
		return nil
	}
	tf.writer.Close(tf.buffer)

	// Flush to disk
	if tf.offset-tf.header.ItemStart > 2 {
		if _, err := tf.file.Seek(-2, io.SeekCurrent); err != nil {
			return err
		}
		tf.offset -= 2
	} else if tf.offset-tf.header.ItemStart > 1 {
		if _, err := tf.file.Seek(-1, io.SeekCurrent); err != nil {
			return err
		}
		tf.offset -= 1
	}
	if _, err := tf.file.Write(tf.buffer.Bytes()); err != nil {
		return fmt.Errorf("error writing data block to file: %v", err)
	}
	tf.offset += int64(len(tf.buffer.Bytes()))

	// Flush to block
	if tf.block != nil {
		tf.block.TrimTip(2)
		tf.block.WriteBytes(tf.buffer.Bytes())
	}

	tf.buffer = tf.buffer.CloneTip(2)
	// Re-open stream
	if err := tf.writer.Open(tf.buffer); err != nil {
		return err
	}

	return nil
}

func (tf *TickFile) Close() error {
	if tf.write {
		return tf.Flush()
	} else {
		return nil
	}
}

func (tf *TickFile) readHeader() error {
	err := binary.Read(tf.file, nativeEndian, &tf.header)
	if err != nil {
		return err
	}
	if tf.header.MagicValue != 0x0d0e0a0402080502 {
		return fmt.Errorf("byteordermark mismatch")
	}

	for i := 0; i < int(tf.header.SectionCount); i++ {
		var sectionID int32
		err = binary.Read(tf.file, nativeEndian, &sectionID)
		if err != nil {
			return err
		}
		var nextSectionOffset int32
		err = binary.Read(tf.file, nativeEndian, &nextSectionOffset)
		if err != nil {
			return err
		}

		beforeSection, err := tf.file.Seek(0, 1)
		if err != nil {
			return err
		}

		switch sectionID {
		case ITEM_SECTION_ID:
			tf.itemSection = &ItemSection{}
			err = tf.itemSection.Read(tf.file, nativeEndian)
			if err != nil {
				return err
			}

		case CONTENT_DESCRIPTION_SECTION_ID:
			tf.contentDescriptionSection = &ContentDescriptionSection{}
			err = tf.contentDescriptionSection.Read(tf.file, nativeEndian)
			if err != nil {
				return err
			}

		case NAME_VALUE_SECTION_ID:
			tf.nameValueSection = &NameValueSection{}
			err = tf.nameValueSection.Read(tf.file, nativeEndian)
			if err != nil {
				return err
			}

		case TAGS_SECTION_ID:
			tf.tagsSection = &TagsSection{}
			err = tf.tagsSection.Read(tf.file, nativeEndian)
			if err != nil {
				return err
			}

		default:
			return fmt.Errorf("unknown section ID %d", sectionID)
		}

		afterSection, err := tf.file.Seek(0, 1)
		if err != nil {
			return err
		}

		if (afterSection - beforeSection) != int64(nextSectionOffset) {
			return fmt.Errorf("section reads too few or too many bytes")
		}
	}

	return nil
}

func (tf *TickFile) writeHeader() error {
	_, err := tf.file.Seek(0, 0)
	if err != nil {
		return err
	}
	var currOffset int32 = 0
	err = binary.Write(tf.file, nativeEndian, tf.header)
	if err != nil {
		return err
	}
	currOffset += int32(reflect.TypeOf(tf.header).Size())

	if tf.itemSection != nil {
		sectionSize := int32(tf.itemSection.Size())
		err = binary.Write(tf.file, nativeEndian, ITEM_SECTION_ID)
		if err != nil {
			return err
		}
		currOffset += 4
		err = binary.Write(tf.file, nativeEndian, sectionSize)
		if err != nil {
			return err
		}
		currOffset += 4
		err = tf.itemSection.Write(tf.file, nativeEndian)
		if err != nil {
			return err
		}
		currOffset += sectionSize
	}

	if tf.contentDescriptionSection != nil {
		sectionSize := int32(tf.contentDescriptionSection.Size())
		err = binary.Write(tf.file, nativeEndian, CONTENT_DESCRIPTION_SECTION_ID)
		if err != nil {
			return err
		}
		currOffset += 4
		err = binary.Write(tf.file, nativeEndian, sectionSize)
		if err != nil {
			return err
		}
		currOffset += 4
		err = tf.contentDescriptionSection.Write(tf.file, nativeEndian)
		if err != nil {
			return err
		}
		currOffset += sectionSize
	}

	if tf.nameValueSection != nil {
		sectionSize := int32(tf.nameValueSection.Size())
		err = binary.Write(tf.file, nativeEndian, NAME_VALUE_SECTION_ID)
		if err != nil {
			return err
		}
		currOffset += 4
		err = binary.Write(tf.file, nativeEndian, sectionSize)
		if err != nil {
			return err
		}
		currOffset += 4
		err = tf.nameValueSection.Write(tf.file, nativeEndian)
		if err != nil {
			return err
		}
		currOffset += sectionSize
	}

	if tf.tagsSection != nil {
		sectionSize := int32(tf.tagsSection.Size())
		err = binary.Write(tf.file, nativeEndian, TAGS_SECTION_ID)
		if err != nil {
			return err
		}
		currOffset += 4
		err = binary.Write(tf.file, nativeEndian, sectionSize)
		if err != nil {
			return err
		}
		currOffset += 4
		err = tf.tagsSection.Write(tf.file, nativeEndian)
		if err != nil {
			return err
		}
		currOffset += sectionSize
	}

	var paddingByte uint8 = 0
	for int64(currOffset) != tf.header.ItemStart {
		err = binary.Write(tf.file, nativeEndian, paddingByte)
		if err != nil {
			return err
		}
		currOffset += 1
	}

	return nil
}

// Check if the data type corresponds to the file description
func (tf *TickFile) checkDataType() error {
	if tf.dataType.Kind() == reflect.Struct {
		n := tf.dataType.NumField()
		var fields []reflect.StructField
		for i := 0; i < n; i++ {
			if tf.dataType.Field(i).Name != "_" {
				fields = append(fields, tf.dataType.Field(i))
			}
		}
		if len(fields) != len(tf.itemSection.Fields) {
			return fmt.Errorf("given type has %d fields, was expecting %d", len(fields), len(tf.itemSection.Fields))
		}
		for i := 0; i < len(fields); i++ {
			dataField := fields[i]
			fileField := tf.itemSection.Fields[i]
			if dataField.Type.Kind() != fieldTypeToKind[fileField.Type] {
				return fmt.Errorf("was not expecting %v", dataField.Type)
			}
			if dataField.Offset != uintptr(fileField.Offset) {
				return fmt.Errorf(
					"got different offsets for field %d: %d %d",
					i,
					dataField.Offset,
					fileField.Offset)
			}
		}
	} else {
		if len(tf.itemSection.Fields) != 1 {
			return fmt.Errorf("got a basic type, was expecting a struct")
		}
		if tf.dataType.Name() != tf.itemSection.Fields[0].Name {
			return fmt.Errorf("got different name, was expecting %s got %s",
				tf.itemSection.Fields[0].Name,
				tf.dataType.Name())
		}
		if tf.dataType.Kind() != fieldTypeToKind[tf.itemSection.Fields[0].Type] {
			return fmt.Errorf("got different type, was expecting %s, got %s",
				fieldTypeToKind[tf.itemSection.Fields[0].Type].String(),
				tf.dataType.Kind().String())
		}
	}

	return nil
}
