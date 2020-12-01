package gotickfile

import (
	"github.com/melaurent/gotickfile/v2/compress"
	"io"
	"reflect"
	"time"
)

type TickReader interface {
	Next() (uint64, TickDeltas, error)
	NextTimeout(time.Duration) (uint64, TickDeltas, error)
}

type CTickReader struct {
	tick     uint64
	nextTick uint64
	ch       chan bool
	br       *compress.BitReader
	info     *ItemSection
	typ      reflect.Type
	tickC    *compress.TickDecompress
	structC  *StructDecompress
	closed   bool
}

type CTickReaderState struct {
	tick     uint64
	nextTick uint64
	br       compress.BitReaderState
}

func NewCTickReader(info *ItemSection, typ reflect.Type, br *compress.BitReader, ch chan bool) (*CTickReader, error) {
	r := &CTickReader{
		tick:    0,
		ch:      ch,
		br:      br,
		info:    info,
		typ:     typ,
		tickC:   nil,
		structC: nil,
		closed:  false,
	}

	return r, nil
}

func (r *CTickReader) State() CTickReaderState {
	return CTickReaderState{
		tick:     r.tick,
		nextTick: r.nextTick,
		br:       r.br.State(),
	}
}

func (r *CTickReader) Reset(state CTickReaderState) {
	r.tick = state.tick
	r.nextTick = state.nextTick
	r.br.Reset(state.br)
}

func (r *CTickReader) Next() (uint64, TickDeltas, error) {
	delta := TickDeltas{
		Pointer: nil,
		Len:     0,
	}
	if r.br.End() {
		if r.closed {
			return r.tick, delta, io.EOF
		} else {
			return r.tick, delta, ErrStreamClosed
		}
	}
	var err error
	if r.tickC == nil {
		// First next
		r.tickC, r.tick, err = compress.NewTickDecompress(r.br)
		if err != nil {
			if err == io.EOF {
				return r.tick, delta, io.ErrUnexpectedEOF
			} else {
				return r.tick, delta, err
			}
		}
		r.structC, delta.Pointer, err = NewStructDecompress(r.info, r.typ, r.br)
		if err != nil {
			if err == io.EOF {
				return r.tick, delta, io.ErrUnexpectedEOF
			} else {
				return r.tick, delta, err
			}
		}

		delta.Len = 1

		if r.br.End() {
			r.nextTick = 0
			return r.tick, delta, nil
		}
		// Read next tick
		r.nextTick, err = r.tickC.Decompress(r.br)
		if err != nil {
			if err == io.EOF {
				return r.tick, delta, io.ErrUnexpectedEOF
			} else {
				return r.tick, delta, err
			}
		}
		for r.tick == r.nextTick {
			delta.Pointer, err = r.structC.Decompress(r.br)
			if err != nil {
				if err == io.EOF {
					return r.tick, delta, io.ErrUnexpectedEOF
				} else {
					return r.tick, delta, err
				}
			}
			delta.Len += 1
			if r.br.End() {
				r.nextTick = 0
				return r.tick, delta, nil
			}
			r.nextTick, err = r.tickC.Decompress(r.br)
			if err != nil {
				if err == io.EOF {
					return r.tick, delta, io.ErrUnexpectedEOF
				} else {
					return r.tick, delta, err
				}
			}
		}

		return r.tick, delta, nil
	} else {
		// How can we know, here, if the tick was read last time ?
		// If nextTick is zero, we failed reading nextTick last time
		// retry
		if r.nextTick == 0 {
			var err error
			if r.br.End() {
				r.nextTick = 0
				return r.tick, delta, nil
			}
			r.nextTick, err = r.tickC.Decompress(r.br)
			if err != nil {
				if err == io.EOF {
					return r.tick, delta, io.ErrUnexpectedEOF
				} else {
					return r.tick, delta, err
				}
			}
		}
		r.structC.Clear()
		r.tick = r.nextTick
		var err error
		for r.tick == r.nextTick {
			delta.Pointer, err = r.structC.Decompress(r.br)
			if err != nil {
				if err == io.EOF {
					return r.tick, delta, io.ErrUnexpectedEOF
				} else {
					return r.tick, delta, err
				}
			}
			delta.Len += 1
			if r.br.End() {
				r.nextTick = 0
				return r.tick, delta, nil
			}
			r.nextTick, err = r.tickC.Decompress(r.br)
			if err != nil {
				if err == io.EOF {
					return r.tick, delta, io.ErrUnexpectedEOF
				} else {
					return r.tick, delta, err
				}
			}
		}

		return r.tick, delta, nil
	}
}

func (r *CTickReader) NextTimeout(dur time.Duration) (uint64, TickDeltas, error) {
	tick, deltas, err := r.Next()
	if err == io.EOF {
		select {
		case res := <-r.ch:
			if res {
				r.closed = true
			}
			return r.Next()
		case <-time.After(dur):
			return 0, TickDeltas{}, ErrReadTimeout
		}
	} else {
		return tick, deltas, err
	}
}

type ChunkReader struct {
	ch     chan bool
	r      *compress.ChunkReader
	Chunk  []byte
	closed bool
}

func NewChunkReader(br *compress.ChunkReader, ch chan bool) *ChunkReader {
	return &ChunkReader{
		ch:     ch,
		r:      br,
		Chunk:  nil,
		closed: false,
	}
}

func (r *ChunkReader) Next() error {
	chunk := r.r.ReadChunk()
	if chunk == nil {
		if r.closed {
			return ErrStreamClosed
		} else {
			return io.EOF
		}
	}
	r.Chunk = chunk
	return nil
}

func (r *ChunkReader) NextTimeout(dur time.Duration) error {
	err := r.Next()
	if err == io.EOF {
		select {
		case res := <-r.ch:
			if res {
				return r.Next()
			} else {
				r.closed = true
				return ErrStreamClosed
			}
		case <-time.After(dur):
			return ErrReadTimeout
		}
	} else {
		return err
	}
}
