package gotickfile

import (
	"fmt"
	"github.com/melaurent/gotickfile/v2/compress"
	"io"
	"reflect"
	"time"
)

type CTickReader struct {
	Tick     uint64
	Val      TickDeltas
	nextTick uint64
	ch       chan bool
	br       *compress.BitReader
	info     *ItemSection
	typ      reflect.Type
	tickC    *compress.TickDecompress
	structC  *StructDecompress
}

func NewCTickReader(info *ItemSection, typ reflect.Type, br *compress.BitReader, ch chan bool) (*CTickReader, error) {
	r := &CTickReader{
		Tick: 0,
		Val: TickDeltas{
			Pointer: nil,
			Len:     1,
		},
		ch:      ch,
		br:      br,
		info:    info,
		typ:     typ,
		tickC:   nil,
		structC: nil,
	}

	return r, nil
}

func (r *CTickReader) Next() error {
	if r.br.End() {
		return io.EOF
	}
	if r.tickC == nil {
		// First next
		tickC, tick, err := compress.NewTickDecompress(r.br)
		if err != nil {
			return fmt.Errorf("error decompressing first tick: %v", err)
		}
		structC, ptr, err := NewStructDecompress(r.info, r.typ, r.br)
		if err != nil {
			return fmt.Errorf("error decompressing first struct: %v", err)
		}

		r.Tick = tick
		r.tickC = tickC
		r.structC = structC

		r.Val.Pointer = ptr
		r.Val.Len = 1

		if r.br.End() {
			r.nextTick = 0
			return nil
		}
		// Read next tick
		r.nextTick, err = tickC.Decompress(r.br)
		if err != nil {
			return fmt.Errorf("error decompressing tick: %v", err)
		}
		for r.Tick == r.nextTick {
			r.Val.Pointer, err = structC.Decompress(r.br)
			if err != nil {
				return fmt.Errorf("error decompressing struct: %v", err)
			}
			r.Val.Len += 1
			if r.br.End() {
				r.nextTick = 0
				return nil
			}
			r.nextTick, err = tickC.Decompress(r.br)
			if err != nil {
				return fmt.Errorf("error decompressing tick: %v", err)
			}
		}

		return nil
	} else {
		// How can we know, here, if the tick was read last time ?
		// If nextTick is zero, we failed reading nextTick last time
		// retry
		if r.nextTick == 0 {
			var err error
			if r.br.End() {
				r.nextTick = 0
				return nil
			}
			r.nextTick, err = r.tickC.Decompress(r.br)
			if err != nil {
				return fmt.Errorf("error decompressing tick: %v", err)
			}
		}
		r.structC.Clear()
		r.Val.Len = 0
		r.Tick = r.nextTick
		var err error
		for r.Tick == r.nextTick {
			r.Val.Pointer, err = r.structC.Decompress(r.br)
			if err != nil {
				return fmt.Errorf("error decompressing struct: %v", err)
			}
			r.Val.Len += 1
			if r.br.End() {
				r.nextTick = 0
				return nil
			}
			r.nextTick, err = r.tickC.Decompress(r.br)
			if err != nil {
				return fmt.Errorf("error decompressing tick: %v", err)
			}
		}

		return nil
	}
}

func (r *CTickReader) NextTimeout(dur time.Duration) error {
	err := r.Next()
	if err == io.EOF {
		select {
		case <-r.ch:
			return r.Next()
		case <-time.After(dur):
			return ErrReadTimeout
		}
	} else {
		return err
	}
}

type ChunkReader struct {
	ch    chan bool
	r     *compress.ChunkReader
	chunk []byte
	count uint8
}

func NewChunkReader(br *compress.ChunkReader, ch chan bool) *ChunkReader {
	return &ChunkReader{
		ch:    ch,
		r:     br,
		chunk: nil,
		count: 0,
	}
}

func (r *ChunkReader) Next() error {
	chunk, count := r.r.ReadChunk()
	if chunk == nil {
		return io.EOF
	}
	r.chunk = chunk
	r.count = count
	return nil
}

func (r *ChunkReader) NextTimeout(dur time.Duration) error {
	err := r.Next()
	if err == io.EOF {
		select {
		case <-r.ch:
			return r.Next()
		case <-time.After(dur):
			return ErrReadTimeout
		}
	} else {
		return err
	}
}
