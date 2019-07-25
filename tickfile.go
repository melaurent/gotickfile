package gotickfile

import (
	"encoding/binary"
	"fmt"
	"github.com/melaurent/gotickfile/mmap"
	"os"
	"reflect"
	"unsafe"
)

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
		panic("Could not determine native endianness.")
	}
}

type Header struct {
	MagicValue   int64
	ItemStart    int64
	ItemEnd      int64
	SectionCount int64
}

type TickFile struct {
	mode                      int
	fileName                  string
	file                      *os.File
	mmap                      *mmap.MMapReader
	buffer                    []byte
	bufferIdx                 int
	dataType                  reflect.Type
	header                    Header
	ticks                     []uint64
	itemSection               *ItemSection
	nameValueSection          *NameValueSection
	tagsSection               *TagsSection
	contentDescriptionSection *ContentDescriptionSection
	itemCount                 int
}

func Create(fileName string, configs ...TickFileConfig) (*TickFile, error) {
	f, err := os.Create(fileName)
	if err != nil {
		return nil, err
	}
	tf := &TickFile{
		mode:      os.O_RDWR,
		fileName:  fileName,
		file:      f,
		buffer:    make([]byte, 4096),
		bufferIdx: 0,
	}
	for _, config := range configs {
		config(tf)
	}
	tf.header.SectionCount = 0
	tf.header.ItemStart = int64(reflect.TypeOf(tf.header).Size())
	tf.header.MagicValue = 0x0d0e0a0402080500

	if tf.itemSection != nil {
		err = tf.checkDataType()
		if err != nil { return nil, err }

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
	paddingBytes := 8 - tf.header.ItemStart % 8

	tf.header.ItemStart += paddingBytes
	tf.header.ItemEnd = tf.header.ItemStart

	err = tf.writeHeader()
	if err != nil {
		return nil, err
	}

	tf.itemCount = 0

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

func (tf *TickFile) GetFileName() string {
	return tf.fileName
}

func (tf *TickFile) computeItemCount() int {
	areaSize := tf.header.ItemEnd - tf.header.ItemStart
	buffSize := int64(tf.bufferIdx)
	count := int((areaSize + buffSize) / int64(tf.itemSection.Info.ItemSize))
	// TODO assert tf.itemCount = count
	return count
}

func (tf *TickFile) ItemCount() int {
	return tf.itemCount
}

func OpenWrite(fileName string, dataType reflect.Type) (*TickFile, error) {
	f, err := os.OpenFile(fileName, os.O_RDWR, 0666)
	if err != nil { return nil, err }

	tf := &TickFile{
		mode:      os.O_RDWR,
		fileName:  fileName,
		file:      f,
		dataType:  dataType,
		buffer:    make([]byte, 1024),
		bufferIdx: 0,
	}

	err = tf.readHeader()
	if err != nil { return nil, err }

	tf.itemCount = tf.computeItemCount()

	err = tf.readTicks()
	if err != nil { return nil, err }

	err = tf.checkDataType()
	if err != nil { return nil, err }

	err = tf.seekItemEnd()
	if err != nil { return nil, err }

	return tf, nil
}

// ticks: ticks since epoch
func (tf *TickFile) Write(tick uint64, val interface{}) error {
	if tf.mode == os.O_RDONLY {
		return fmt.Errorf("writing in reading mode not supported")
	}
	if tf.itemSection == nil {
		return fmt.Errorf("this file has no item section")
	}
	if reflect.TypeOf(val) != tf.dataType {
		val = reflect.ValueOf(val).Elem().Interface()
		if reflect.TypeOf(val) != tf.dataType {
			return fmt.Errorf("was expecting %s, got %s", tf.dataType, reflect.TypeOf(val))
		}
	}

	vp := reflect.New(reflect.TypeOf(val))
	vp.Elem().Set(reflect.ValueOf(val))
	ptr := vp.Pointer()
	length := int(tf.itemSection.Info.ItemSize)
	var sl = struct {
		addr uintptr
		len  int
		cap  int
	}{ptr, length, length}
	b := *(*[]byte)(unsafe.Pointer(&sl))
	if tf.bufferIdx + length > len(tf.buffer) {
		err := tf.Flush()
		if err != nil {
			return err
		}
	}
	for i := 0; i < len(b); i++ {
		tf.buffer[tf.bufferIdx] = b[i]
		tf.bufferIdx += 1
	}

	tf.ticks = append(tf.ticks, tick)
	tf.itemCount += 1

	return nil
}

func OpenRead(fileName string, dataType reflect.Type) (*TickFile, error) {
	f, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	tf := &TickFile{
		mode: os.O_RDONLY,
		fileName: fileName,
		file: f,
		dataType: dataType,
	}

	err = tf.readHeader()
	if err != nil { return nil, err }

	err = tf.checkDataType()
	if err != nil { return nil, err }

	tf.itemCount = tf.computeItemCount()

	tf.mmap, err = tf.openReadableMapping()
	if err != nil {
		return nil, fmt.Errorf("error opening file: %v", err)
	}

	err = tf.readTicks()
	if err != nil { return nil, err }

	_, err = tf.file.Seek(tf.header.ItemStart, 0)
	if err != nil {
		return nil, err
	}

	return tf, nil
}

func OpenHeader(fileName string) (*TickFile, error) {
	f, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	tf := &TickFile{
		mode: os.O_RDONLY,
		fileName: fileName,
		file: f,
		bufferIdx: 0,
	}

	if err := tf.readHeader(); err != nil {
		return nil, err
	}

	tf.itemCount = tf.computeItemCount()

	return tf, nil
}

func (tf *TickFile) openReadableMapping() (*mmap.MMapReader, error) {
	if tf.mode == os.O_RDWR {
		return nil, fmt.Errorf("memory mapping in write mode not supported")
	}
	if tf.mmap != nil {
		return tf.mmap, nil
	}

	_, err := tf.file.Seek(0, 0)
	if err != nil { return nil, err }
	if tf.header.ItemStart == tf.header.ItemEnd {
		return nil, fmt.Errorf("error opening readable mapping: no data")
	}
	reader, err := mmap.Open(
		tf.file,
		tf.header.ItemStart,
		int64(tf.itemSection.Info.ItemSize))
	return reader, err
}

/*
func (tf *TickFile) Read() (interface{}, error) {
	if tf.mode == os.O_WRONLY {
		return nil, fmt.Errorf("reading in write mode not supported")
	}
	val := reflect.New(tf.dataType)
	length := int(tf.itemSection.Info.ItemSize)
	var sl = struct {
		addr uintptr
		len  int
		cap  int
	}{val.Pointer(), length, length}
	b := *(*[]byte)(unsafe.Pointer(&sl))
	_, err := tf.file.Read(b)

	return val, err
}
 */


func (tf *TickFile) Read(idx int) (uint64, interface{}, error) {
	if idx >= tf.itemCount {
		return 0, nil, fmt.Errorf("read out of range")
	}
	if tf.mode == os.O_RDONLY {
		// Easy, use mmap
		ptr := tf.mmap.GetItem(0)
		val := reflect.NewAt(tf.dataType, ptr)
		return tf.ticks[idx], val.Interface(), nil
	} else {
		// Inefficient ! We do not expect a lot of reads when file is open in write mode

		val := reflect.New(tf.dataType)
		length := int(tf.itemSection.Info.ItemSize)
		var sl = struct {
			addr uintptr
			len  int
			cap  int
		}{val.Pointer(), length, length}
		b := *(*[]byte)(unsafe.Pointer(&sl))

		areaSize := tf.header.ItemEnd - tf.header.ItemStart
		itemsInFile := int((areaSize) / int64(tf.itemSection.Info.ItemSize))

		if idx >= itemsInFile {
			// Read from buffer
			buffIdx := (idx - itemsInFile) * int(tf.itemSection.Info.ItemSize)
			for i := 0; i < len(b); i++ {
				b[i] = tf.buffer[buffIdx + i]
			}
		} else {
			// Read from file
			if err := tf.seekItem(int64(idx)); err != nil {
				return 0, nil, err
			}

			_, err := tf.file.Read(b)
			if err != nil {
				return 0, nil, err
			}
			// Go back to item end for next writings
			if err = tf.seekItemEnd(); err != nil {
				return 0, nil, err
			}
		}

		return tf.ticks[idx], val.Interface(), nil
	}
}

func (tf *TickFile) seekItem(idx int64) error {
	if tf.itemSection == nil {
		return fmt.Errorf("no item section defined, cannot seek on item size")
	}
	_, err := tf.file.Seek(tf.header.ItemStart + idx * int64(tf.itemSection.Info.ItemSize), 0)
	return err
}

func (tf *TickFile) seekItemEnd() error {
	_, err := tf.file.Seek(tf.header.ItemEnd, 0)
	return err
}


func (tf *TickFile) Flush() error {
	if tf.mode == os.O_RDONLY {
		return fmt.Errorf("writing in read only")
	}

	_, err := tf.file.Write(tf.buffer[0:tf.bufferIdx])
	if err != nil {
		return err
	}

	tf.header.ItemEnd += int64(tf.bufferIdx)

	// Now write header to update itemStart, itemEnd, endEpoch
	if err := tf.writeHeader(); err != nil {
		return err
	}

	if err := tf.writeTicks(); err != nil {
		return err
	}

	// Go back to end of items after writing ticks
	if err := tf.seekItemEnd(); err != nil {
		return err
	}

	tf.bufferIdx = 0
	return nil
}


func (tf *TickFile) Close() error {
	if tf.mode == os.O_RDWR {
		if err := tf.Flush(); err != nil {
			return err
		}
	}
	return tf.file.Close()
}

func (tf *TickFile) readHeader() error {
	err := binary.Read(tf.file, nativeEndian, &tf.header)
	if err != nil { return err }
	if tf.header.MagicValue != 0x0d0e0a0402080500 {
		return fmt.Errorf("byteordermark mismatch")
	}

	for i := 0; i < int(tf.header.SectionCount); i++ {
		var sectionID int32
		err = binary.Read(tf.file, nativeEndian, &sectionID)
		if err != nil { return err }
		var nextSectionOffset int32
		err = binary.Read(tf.file, nativeEndian, &nextSectionOffset)
		if err != nil { return err }

		beforeSection, err := tf.file.Seek(0, 1)
		if err != nil { return err }

		switch sectionID {
		case ITEM_SECTION_ID:
			tf.itemSection = &ItemSection{}
			err = tf.itemSection.Read(tf.file, nativeEndian)
			if err != nil { return err }

		case CONTENT_DESCRIPTION_SECTION_ID:
			tf.contentDescriptionSection = &ContentDescriptionSection{}
			err = tf.contentDescriptionSection.Read(tf.file, nativeEndian)
			if err != nil { return err }

		case NAME_VALUE_SECTION_ID:
			tf.nameValueSection = &NameValueSection{}
			err = tf.nameValueSection.Read(tf.file, nativeEndian)
			if err != nil { return err }

		case TAGS_SECTION_ID:
			tf.tagsSection = &TagsSection{}
			err = tf.tagsSection.Read(tf.file, nativeEndian)
			if err != nil { return err }

		default:
			return fmt.Errorf("unknown section ID %d", sectionID)
		}

		afterSection, err := tf.file.Seek(0, 1)
		if err != nil { return err }
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
	if err != nil { return err }
	currOffset += int32(reflect.TypeOf(tf.header).Size())

	if tf.itemSection != nil {
		sectionSize := int32(tf.itemSection.Size())
		err = binary.Write(tf.file, nativeEndian, ITEM_SECTION_ID)
		if err != nil { return err }
		currOffset += 4
		err = binary.Write(tf.file, nativeEndian, sectionSize)
		if err != nil { return err }
		currOffset += 4
		err = tf.itemSection.Write(tf.file, nativeEndian)
		if err != nil { return err }
		currOffset += sectionSize
	}

	if tf.contentDescriptionSection != nil {
		sectionSize := int32(tf.contentDescriptionSection.Size())
		err = binary.Write(tf.file, nativeEndian, CONTENT_DESCRIPTION_SECTION_ID)
		if err != nil { return err }
		currOffset += 4
		err = binary.Write(tf.file, nativeEndian, sectionSize)
		if err != nil { return err }
		currOffset += 4
		err = tf.contentDescriptionSection.Write(tf.file, nativeEndian)
		if err != nil { return err }
		currOffset += sectionSize
	}

	if tf.nameValueSection != nil {
		sectionSize := int32(tf.nameValueSection.Size())
		err = binary.Write(tf.file, nativeEndian, NAME_VALUE_SECTION_ID)
		if err != nil { return err }
		currOffset += 4
		err = binary.Write(tf.file, nativeEndian, sectionSize)
		if err != nil { return err }
		currOffset += 4
		err = tf.nameValueSection.Write(tf.file, nativeEndian)
		if err != nil { return err }
		currOffset += sectionSize
	}

	if tf.tagsSection != nil {
		sectionSize := int32(tf.tagsSection.Size())
		err = binary.Write(tf.file, nativeEndian, TAGS_SECTION_ID)
		if err != nil { return err }
		currOffset += 4
		err = binary.Write(tf.file, nativeEndian, sectionSize)
		if err != nil { return err }
		currOffset += 4
		err = tf.tagsSection.Write(tf.file, nativeEndian)
		if err != nil { return err }
		currOffset += sectionSize
	}

	var paddingByte uint8 = 0
	for; int64(currOffset) != tf.header.ItemStart; {
		err = binary.Write(tf.file, nativeEndian, paddingByte)
		if err != nil { return err }
		currOffset += 1
	}

	position, err := tf.file.Seek(0, 1)
	if err != nil { return err }

	bytesToSkip := tf.header.ItemStart - position
	_, err = tf.file.Seek(bytesToSkip, 1)
	if err != nil { return err }

	position, err = tf.file.Seek(0, 1)
	if err != nil { return err }

	return nil
}

func (tf *TickFile) readTicks() error {
	var data []byte

	if tf.mmap == nil {
		if err := tf.seekItemEnd(); err != nil {
			return err
		}
		fStats, err := tf.file.Stat()
		if err != nil {
			return err
		}

		ticksSize := fStats.Size() - tf.header.ItemEnd
		data = make([]byte, ticksSize)
		if _, err := tf.file.Read(data); err != nil {
			return err
		}
	} else {
		ticksSize := int(tf.mmap.GetSize()) - tf.itemCount * int(tf.itemSection.Info.ItemSize)

		sh := &reflect.SliceHeader{
			Data: uintptr(tf.mmap.GetItem(tf.itemCount)),
			Len:  ticksSize,
			Cap:  ticksSize,
		}
		data = *(*[]byte)(unsafe.Pointer(sh))
	}

	ticks, err := DecompressTicks(data, tf.itemCount)
	if err != nil {
		return err
	}
	tf.ticks = ticks
	return nil
}

func (tf *TickFile) writeTicks() error {
	err := tf.seekItemEnd()
	if err != nil {
		return err
	}
	data := CompressTicks(tf.ticks)
	_, err = tf.file.Write(data)
	if err != nil {
		return err
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
			return fmt.Errorf("given type has %d fields, was expecting %d", n, len(tf.itemSection.Fields))
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