package gotickfile

import (
	"encoding/binary"
	"fmt"
	gotickfilev1 "github.com/melaurent/gotickfile"
	"github.com/melaurent/kafero"
	"io"
	"reflect"
	"unsafe"
)

func readText(r io.Reader, order binary.ByteOrder) (string, error) {
	bytes, err := readBytes(r, order)
	return string(bytes), err
}

func writeText(w io.Writer, order binary.ByteOrder, text string) error {
	return writeBytes(w, order, []byte(text))
}

func readBytes(r io.Reader, order binary.ByteOrder) ([]byte, error) {
	var length int32
	err := binary.Read(r, order, &length)
	if err != nil {
		return nil, err
	}
	bytes := make([]byte, length)
	err = binary.Read(r, order, bytes)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

func writeBytes(w io.Writer, order binary.ByteOrder, bytes []byte) error {
	var nBytes = int32(len(bytes))
	err := binary.Write(w, order, nBytes)
	if err != nil {
		return err
	}
	return binary.Write(w, order, bytes)
}

func textSize(text string) int64 {
	return 4 + int64(len([]byte(text)))
}

func V1ToV2(dst kafero.File, src kafero.File, typ reflect.Type) error {
	tfv1, err := gotickfilev1.OpenRead(src, typ)
	if err != nil {
		return err
	}
	tags := tfv1.GetTags()
	nv := tfv1.GetNameValues()
	desc := tfv1.GetContentDescription()
	var configs []TickFileConfig
	configs = append(configs, WithDataType(typ))
	if tags != nil {
		configs = append(configs, WithTags(tags))
	}
	if nv != nil {
		configs = append(configs, WithNameValues(nv))
	}
	if desc != nil {
		configs = append(configs, WithContentDescription(*desc))
	}
	tfv2, err := Create(dst, configs...)
	if err != nil {
		_ = tfv1.Close()
		return fmt.Errorf("error creating tickfile v2: %w", err)
	}
	N := tfv1.ItemCount()
	for i := 0; i < N; i++ {
		tick, delta, err := tfv1.ReadItem(i)
		if err != nil {
			_ = tfv1.Close()
			_ = tfv2.Close()
			return fmt.Errorf("error reading tickfile v1: %w", err)
		}

		ptr := reflect.ValueOf(delta).Pointer()
		if err := tfv2.Write(tick, TickDeltas{
			Pointer: unsafe.Pointer(ptr),
			Len:     1,
		}); err != nil {
			_ = tfv1.Close()
			_ = tfv2.Close()
			return fmt.Errorf("error copying to tickfile v2: %w", err)
		}
	}

	if err := tfv2.Close(); err != nil {
		_ = tfv1.Close()
		return fmt.Errorf("error closing tickfile v2: %w", err)
	}
	if err := tfv1.Close(); err != nil {
		return fmt.Errorf("error closing tickfile v1: %w", err)
	}

	return nil
}
