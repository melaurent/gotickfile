package gotickfile

import (
	"encoding/binary"
	"io"
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
