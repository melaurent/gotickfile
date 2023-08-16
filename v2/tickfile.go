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
	lastWrite                 int
	block                     *compress.BBuffer
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
		return nil, fmt.Errorf("error truncating file: %w", err)
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
	tf.lastWrite = 0
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

func (tf *TickFile) GetContentDescription() *string {
	if tf.contentDescriptionSection != nil {
		return &tf.contentDescriptionSection.ContentDescription
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

	if _, err := tf.file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("error seeking to beginning of file: %w", err)
	}

	if err := tf.readHeader(); err != nil {
		return nil, err
	}

	if err := tf.checkDataType(); err != nil {
		return nil, err
	}

	if _, err := tf.file.Seek(tf.header.ItemStart, io.SeekStart); err != nil {
		return nil, err
	}
	tf.offset = tf.header.ItemStart

	block, err := ioutil.ReadAll(tf.file)
	if err != nil {
		return nil, err
	}

	tf.offset += int64(len(block))
	tf.lastWrite = len(block)

	if len(block) == 0 {
		tf.block = compress.NewBBuffer(nil, 0)
		tf.lastTick = 0
	} else {
		// Create buffer from block
		tf.block, err = blockToBuffer(block)
		if err != nil {
			return nil, err
		}
		// Read to the end
		w, lastTick, err := CTickWriterFromBlock(tf.itemSection, tf.dataType, tf.block)
		if err != nil {
			return nil, fmt.Errorf("error loading writer from block: %w", err)
		}
		// Open block
		if err := w.Open(tf.block); err != nil {
			return nil, fmt.Errorf("error opening block for writing: %w", err)
		}
		tf.writer = w
		tf.lastTick = lastTick
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
	var v uint16
	N := len(block)
	if N > 1 {
		v = uint16(block[N-2])<<8 | uint16(block[N-1])
	} else {
		v = uint16(block[N-1])
	}

	var count uint8 = 0
	for v != 0 && v&0x1F != 0x1F {
		v >>= 1
		count += 1
	}
	if v == 0 {
		return nil, fmt.Errorf("no EOF found in block")
	} else {
		buf := compress.NewBBuffer(block, count)
		return buf, nil
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
		fmt.Println("TF OUT OF ORDER", tick, tf.lastTick)
		return ErrTickOutOfOrder
	}
	count := val.Len

	if count == 0 {
		return nil
	}

	size := tf.dataType.Size()
	ptr := val.Pointer
	if tf.writer == nil {
		tf.block.Lock()
		tf.writer = NewCTickWriter(tf.itemSection, tick, ptr, tf.block)
		tf.block.Unlock()
		count -= 1
		if count > 0 {
			ptr = unsafe.Pointer(uintptr(ptr) + size)
		}
	}

	tf.block.Lock()
	for i := 0; i < count; i++ {
		tf.writer.Write(tick, ptr, tf.block)
		if i < count-1 {
			ptr = unsafe.Pointer(uintptr(ptr) + size)
		}
	}
	tf.block.Unlock()

	tf.lastTick = tick

	return nil
}

func OpenRead(file kafero.File, dataType reflect.Type) (*TickFile, error) {
	tf := &TickFile{
		file:     file,
		write:    false,
		dataType: dataType,
	}

	if _, err := tf.file.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("error seeking to beginning of file: %w", err)
	}

	if err := tf.readHeader(); err != nil {
		return nil, fmt.Errorf("error reading header: %w", err)
	}

	if err := tf.checkDataType(); err != nil {
		return nil, fmt.Errorf("error checking data type: %w", err)
	}

	if _, err := tf.file.Seek(tf.header.ItemStart, 0); err != nil {
		return nil, fmt.Errorf("error seeking to first item: %w", err)
	}
	tf.offset = tf.header.ItemStart
	// Read file to block
	block, err := ioutil.ReadAll(tf.file)
	if err != nil {
		return nil, fmt.Errorf("error reading file to block: %w", err)
	}
	tf.offset += int64(len(block))
	tf.lastWrite = len(block)
	if len(block) == 0 {
		tf.block = compress.NewBBuffer(nil, 0)
	} else {
		// Create buffer from block
		tf.block, err = blockToBuffer(block)
		if err != nil {
			return nil, err
		}
		// Open block
		if err := tf.block.Rewind(5); err != nil {
			return nil, err
		}
		tr, err := NewCTickReader(tf.itemSection, tf.dataType, compress.NewBitReader(tf.block))
		if err != nil {
			return nil, fmt.Errorf("error getting tick reader: %w", err)
		}
		err = nil
		var tick uint64 = 0
		//var delta TickDeltas
		type RawTradeDelta struct {
			Part1       uint64
			RawQuantity uint64
			ID          uint64
			AggregateID uint64
		}
		for err == nil {
			tick, _, err = tr.Next()
			//fmt.Println(tick, (*RawTradeDelta)(delta.Pointer))
		}
		if err == io.EOF {
			tf.lastTick = tick
		} else {
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
		return nil, fmt.Errorf("error seeking to beginning of file: %w", err)
	}

	if err := tf.readHeader(); err != nil {
		return nil, err
	}

	return tf, nil
}

func (tf *TickFile) LastTick() uint64 {
	return tf.lastTick
}

func (tf *TickFile) GetTickReader() (*CTickReader, error) {
	return NewCTickReader(tf.itemSection, tf.dataType, compress.NewBitReader(tf.block))
}

func (tf *TickFile) GetChunkReader(chunkSize int) (*compress.ChunkReader, error) {
	return compress.NewChunkReader(tf.block, chunkSize), nil
}

func (tf *TickFile) Flush() error {
	if tf.writer == nil {
		return nil
	}

	// Flush to disk
	if tf.offset-tf.header.ItemStart > 2 {
		if _, err := tf.file.Seek(-2, io.SeekCurrent); err != nil {
			return err
		}
		tf.offset -= 2
		tf.lastWrite -= 2
	} else if tf.offset-tf.header.ItemStart > 1 {
		if _, err := tf.file.Seek(-1, io.SeekCurrent); err != nil {
			return err
		}
		tf.offset -= 1
		tf.lastWrite -= 1
	}

	tf.block.Lock()
	tf.writer.Close(tf.block)
	n, err := tf.file.Write(tf.block.Bytes()[tf.lastWrite:])
	if err != nil {
		return fmt.Errorf("error writing data block to file: %w", err)
	}
	tf.offset += int64(n)
	tf.lastWrite += n

	// Re-open stream
	if err := tf.writer.Open(tf.block); err != nil {
		return err
	}

	tf.block.Unlock()

	if err := tf.file.Sync(); err != nil {
		return fmt.Errorf("error syncing file")
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
		if tf.header.MagicValue == 0x0d0e0a0402080500 {
			return ErrTickFileV1
		}
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
		section, err := TypeToItemSection(tf.dataType)
		if err != nil {
			return fmt.Errorf("error converting type to item section: %w", err)
		}
		if len(section.Fields) != len(tf.itemSection.Fields) {
			return fmt.Errorf("given type has %d fields, was expecting %d", len(section.Fields), len(tf.itemSection.Fields))
		}
		for i := range section.Fields {
			dataField := section.Fields[i]
			fileField := tf.itemSection.Fields[i]
			if dataField.Type != fileField.Type {
				return fmt.Errorf("was not expecting %v", dataField.Type)
			}
			if dataField.Offset != fileField.Offset {
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
