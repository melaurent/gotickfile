package gotickfile

import (
	"encoding/binary"
	"fmt"
	"github.com/melaurent/kafero"
	"io"
	"reflect"
	"syscall"
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

type Header struct {
	MagicValue   int64
	ItemStart    int64
	ItemEnd      int64
	SectionCount int64
}

type TickFile struct {
	Ticks                     []uint64
	file                      kafero.File
	write                     bool
	mmap                      []byte
	buffer                    []byte
	bufferIdx                 int
	dataType                  reflect.Type
	header                    Header
	itemSection               *ItemSection
	nameValueSection          *NameValueSection
	tagsSection               *TagsSection
	contentDescriptionSection *ContentDescriptionSection
	itemCount                 int
}

func Create(file kafero.File, configs ...TickFileConfig) (*TickFile, error) {
	var tf *TickFile

	if err := file.Truncate(0); err != nil {
		return nil, fmt.Errorf("error truncating file: %v", err)
	}

	tf = &TickFile{
		file:      file,
		write:     true,
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
	tf.header.ItemEnd = tf.header.ItemStart

	if err := tf.writeHeader(); err != nil {
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

func OpenWrite(file kafero.File, dataType reflect.Type) (*TickFile, error) {
	tf := &TickFile{
		file:      file,
		write:     true,
		dataType:  dataType,
		buffer:    make([]byte, 1024),
		bufferIdx: 0,
	}

	if _, err := tf.file.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("error seeking to beginning of file: %v", err)
	}

	if err := tf.readHeader(); err != nil {
		return nil, err
	}

	tf.itemCount = tf.computeItemCount()

	if err := tf.readTicks(); err != nil {
		return nil, err
	}

	if err := tf.checkDataType(); err != nil {
		return nil, err
	}

	if _, err := tf.file.Seek(tf.header.ItemStart, 0); err != nil {
		return nil, err
	}

	return tf, nil
}

// ticks: ticks since epoch
func (tf *TickFile) Write(tick uint64, val interface{}) error {
	if !tf.write {
		return fmt.Errorf("writing in read only tickfile")
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
	if N := len(tf.Ticks); N > 0 && tick < tf.Ticks[N-1] {
		return fmt.Errorf("out of order tick write not supported")
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
	if tf.bufferIdx+length > len(tf.buffer) {
		err := tf.Flush()
		if err != nil {
			return err
		}
	}
	for i := 0; i < len(b); i++ {
		tf.buffer[tf.bufferIdx] = b[i]
		tf.bufferIdx += 1
	}

	tf.Ticks = append(tf.Ticks, tick)
	tf.itemCount += 1

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

	tf.itemCount = tf.computeItemCount()

	if tf.file.CanMmap() && tf.ItemCount() > 0 {
		mmap, err := tf.openReadableMapping()
		if err != nil {
			return nil, fmt.Errorf("error opening readable mapping: %v", err)
		}
		tf.mmap = mmap
	}

	if err := tf.readTicks(); err != nil {
		return nil, fmt.Errorf("error reading ticks: %v", err)
	}

	if _, err := tf.file.Seek(tf.header.ItemStart, 0); err != nil {
		return nil, fmt.Errorf("error seeking to first item: %v", err)

	}

	return tf, nil
}

func OpenHeader(file kafero.File) (*TickFile, error) {

	tf := &TickFile{
		file:      file,
		bufferIdx: 0,
	}

	if _, err := tf.file.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("error seeking to beginning of file: %v", err)
	}

	if err := tf.readHeader(); err != nil {
		return nil, err
	}

	tf.itemCount = tf.computeItemCount()

	return tf, nil
}

func (tf *TickFile) openReadableMapping() ([]byte, error) {
	if tf.mmap != nil {
		return tf.mmap, nil
	}

	_, err := tf.file.Seek(0, 0)
	if err != nil {
		return nil, err
	}
	if tf.header.ItemStart == tf.header.ItemEnd {
		return nil, fmt.Errorf("error opening readable mapping: no data")
	}

	fi, err := tf.file.Stat()
	if err != nil {
		return nil, err
	}

	fSize := fi.Size()
	if fSize == 0 {
		return nil, nil
	}
	if fSize < 0 {
		return nil, fmt.Errorf("mmap: file has negative size")
	}
	if fSize != int64(int(fSize)) {
		return nil, fmt.Errorf("mmap: file is too large")
	}

	data, err := tf.file.Mmap(
		0,
		int(fSize),
		syscall.PROT_READ,
		syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("error opening mmap: %v", err)
	}

	return data, nil
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

func (tf *TickFile) Read(idx int) (uint64, []interface{}, error) {
	// return all the items associated with the given tick
	if idx >= tf.itemCount {
		return 0, nil, io.EOF
	}
	tick := tf.Ticks[idx]
	var items []interface{}
	for ; idx < len(tf.Ticks) && tf.Ticks[idx] == tick; idx++ {
		_, item, err := tf.ReadItem(idx)
		if err != nil {
			return 0, nil, err
		}
		items = append(items, item)
	}
	return tick, items, nil
}

func (tf *TickFile) ReadItem(idx int) (uint64, interface{}, error) {
	if idx >= tf.itemCount {
		return 0, nil, io.EOF
	}
	if tf.mmap != nil {
		// Easy, use mmap
		mmapIdx := tf.header.ItemStart + (int64(idx) * int64(tf.itemSection.Info.ItemSize))
		ptr := unsafe.Pointer(&tf.mmap[mmapIdx])
		val := reflect.NewAt(tf.dataType, ptr)
		return tf.Ticks[idx], val.Interface(), nil
	} else {

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
				b[i] = tf.buffer[buffIdx+i]
			}
		} else {
			// Read from file

			pointer := tf.header.ItemStart + int64(idx)*int64(tf.itemSection.Info.ItemSize)
			if _, err := tf.file.Seek(pointer, 0); err != nil {
				return 0, nil, err
			}

			if _, err := tf.file.Read(b); err != nil {
				return 0, nil, err
			}

			if tf.write {
				// Inefficient ! We do not expect a lot of reads when file is open in write mode
				// Go back to item end for next writings
				if _, err := tf.file.Seek(tf.header.ItemEnd, 0); err != nil {
					return 0, nil, err
				}
			}
		}

		return tf.Ticks[idx], val.Interface(), nil
	}
}

func (tf *TickFile) Flush() error {
	if tf.file == nil {
		return fmt.Errorf("teafile not open")
	}
	if !tf.write {
		return fmt.Errorf("writing in read only tickfile")
	}

	if _, err := tf.file.Write(tf.buffer[0:tf.bufferIdx]); err != nil {
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
	if _, err := tf.file.Seek(tf.header.ItemEnd, 0); err != nil {
		return err
	}

	tf.bufferIdx = 0
	return nil
}

func (tf *TickFile) Close() error {
	if tf.file != nil && tf.write {
		return tf.Flush()
	}
	if tf.mmap != nil {
		if err := tf.file.Munmap(); err != nil {
			return fmt.Errorf("error munmapping: %v", err)
		}
		tf.mmap = nil
	}
	return nil
}

func (tf *TickFile) readHeader() error {
	err := binary.Read(tf.file, nativeEndian, &tf.header)
	if err != nil {
		return err
	}
	if tf.header.MagicValue != 0x0d0e0a0402080500 {
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

func (tf *TickFile) readTicks() error {
	var data []byte

	if tf.mmap == nil {
		if _, err := tf.file.Seek(tf.header.ItemEnd, 0); err != nil {
			return err
		}
		fstat, err := tf.file.Stat()
		if err != nil {
			return fmt.Errorf("error getting file info: %v", err)
		}

		size := fstat.Size()
		ticksSize := size - tf.header.ItemEnd
		data = make([]byte, ticksSize)
		if _, err := tf.file.Read(data); err != nil {
			return err
		}
	} else {

		ticksSize := len(tf.mmap) - tf.itemCount*int(tf.itemSection.Info.ItemSize)
		mmapIdx := tf.header.ItemStart + (int64(tf.itemCount) * int64(tf.itemSection.Info.ItemSize))
		sh := &reflect.SliceHeader{
			Data: uintptr(unsafe.Pointer(&tf.mmap[mmapIdx])),
			Len:  ticksSize,
			Cap:  ticksSize,
		}
		data = *(*[]byte)(unsafe.Pointer(sh))
	}

	ticks, err := DecompressTicks(data, tf.itemCount)
	if err != nil {
		return err
	}
	tf.Ticks = ticks
	return nil
}

func (tf *TickFile) writeTicks() error {
	if _, err := tf.file.Seek(tf.header.ItemEnd, 0); err != nil {
		return err
	}
	data := CompressTicks(tf.Ticks)
	if _, err := tf.file.Write(data); err != nil {
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
