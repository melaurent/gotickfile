package gotickfile

import (
	"fmt"
	"github.com/dsnet/golib/memfile"
	"io"
	"os"
	"syscall"
)

type FileHandle interface {
	Read() (io.ReadSeeker, error)
	Write() (io.WriteSeeker, error)
	ReadWrite() (io.ReadWriteSeeker, error)
	Delete() error
	MMap() ([]byte, error)
	Size() (int64, error)
	Close() error
}

type OSFileHandle struct {
	fileName string
	file     *os.File
	mode     int
	mmap     []byte
}

func NewOSFileHandle(fileName string) *OSFileHandle {
	return &OSFileHandle{
		fileName: fileName,
		file:     nil,
		mode:     0,
		mmap:     nil,
	}
}

func (fh *OSFileHandle) Read() (io.ReadSeeker, error) {
	if fh.file != nil {
		if fh.mode == os.O_RDWR || fh.mode == os.O_RDONLY {
			return fh.file, nil
		} else {
			return nil, fmt.Errorf("file already open and not in read mode")
		}
	} else {
		file, err := os.Open(fh.fileName)
		if err != nil {
			return nil, fmt.Errorf("error opening file for reading: %v", err)
		}
		fh.file = file
		return fh.file, nil
	}
}

func (fh *OSFileHandle) Write() (io.WriteSeeker, error) {
	if fh.file != nil {
		if fh.mode == os.O_RDWR || fh.mode == os.O_WRONLY {
			return fh.file, nil
		} else {
			return nil, fmt.Errorf("file already open and not in write mode")
		}
	} else {
		file, err := os.OpenFile(fh.fileName, os.O_WRONLY|os.O_CREATE, 0666)
		if err != nil {
			return nil, fmt.Errorf("error opening file for reading: %v", err)
		}
		fh.file = file
		return fh.file, nil
	}
}

func (fh *OSFileHandle) ReadWrite() (io.ReadWriteSeeker, error) {
	if fh.file != nil {
		if fh.mode == os.O_RDWR || fh.mode == os.O_WRONLY {
			return fh.file, nil
		} else {
			return nil, fmt.Errorf("file already open and not in write mode")
		}
	} else {
		file, err := os.OpenFile(fh.fileName, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return nil, fmt.Errorf("error opening file for reading: %v", err)
		}
		fh.file = file
		return fh.file, nil
	}
}

func (fh *OSFileHandle) Delete() error {
	_, err := os.Stat(fh.fileName)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		} else {
			return err
		}
	} else {
		return os.Remove(fh.fileName)
	}
}

func (fh *OSFileHandle) MMap() ([]byte, error) {
	if fh.file == nil {
		return nil, fmt.Errorf("mmap: file is closed")
	}

	if fh.mode == os.O_WRONLY {
		return nil, fmt.Errorf("mmap: file is in write only")
	}

	fi, err := fh.file.Stat()
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

	data, err := syscall.Mmap(int(fh.file.Fd()), 0, int(fSize), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (fh *OSFileHandle) Size() (int64, error) {
	var fstat os.FileInfo
	if fh.file != nil {
		var err error
		fstat, err = fh.file.Stat()
		if err != nil {
			return 0, fmt.Errorf("error getting file stat: %v", err)
		}
	} else {
		var err error
		fstat, err = os.Stat(fh.fileName)
		if err != nil {
			return 0, fmt.Errorf("error getting file stat: %v", err)
		}
	}

	return fstat.Size(), nil
}

func (fh *OSFileHandle) Close() error {
	if fh.mmap != nil {
		if err := syscall.Munmap(fh.mmap); err != nil {
			return fmt.Errorf("error closing mmap: %v", err)
		}
		fh.mmap = nil
	}
	if fh.file != nil {
		if err := fh.file.Close(); err != nil {
			return fmt.Errorf("error closing file: %v", err)
		}
		fh.file = nil
	}

	return nil
}

type MemFileHandle struct {
	file *memfile.File
}

func NewMemFileHandle() *MemFileHandle {
	return &MemFileHandle{
		file: nil,
	}
}

func (fh *MemFileHandle) Read() (io.ReadSeeker, error) {
	if fh.file != nil {
		return fh.file, nil
	} else {
		file := memfile.New(nil)
		fh.file = file
		return fh.file, nil
	}
}

func (fh *MemFileHandle) Write() (io.WriteSeeker, error) {
	if fh.file != nil {
		return fh.file, nil
	} else {
		file := memfile.New(nil)
		fh.file = file
		return fh.file, nil
	}
}

func (fh *MemFileHandle) ReadWrite() (io.ReadWriteSeeker, error) {
	if fh.file != nil {
		return fh.file, nil
	} else {
		file := memfile.New(nil)
		fh.file = file
		return fh.file, nil
	}
}

func (fh *MemFileHandle) Delete() error {
	fh.file = nil
	return nil
}

func (fh *MemFileHandle) MMap() ([]byte, error) {
	if fh.file == nil {
		return nil, fmt.Errorf("mmap on a closed file")
	}
	return fh.file.Bytes(), nil
}

func (fh *MemFileHandle) Size() (int64, error) {
	if fh.file == nil {
		return 0, fmt.Errorf("closed file has no size")
	}
	return int64(len(fh.file.Bytes())), nil
}

func (fh *MemFileHandle) Close() error {
	if fh.file != nil {
		if _, err := fh.file.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf("error seeking to beginning of file")
		}
	}
	return nil
}
