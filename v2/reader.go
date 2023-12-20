package gotickfile

import (
	"fmt"
	"github.com/melaurent/gotickfile/v2/compress"
	"io"
	"reflect"
)

type CTickReader struct {
	tick     uint64
	nextTick uint64
	ch       chan bool
	br       *compress.BitReader
	info     *ItemSection
	typ      reflect.Type
	tickC    *compress.TickDecompress
	structC  *StructDecompress
}

type CTickReaderState struct {
	tick     uint64
	nextTick uint64
	br       compress.BitReaderState
}

func NewCTickReader(info *ItemSection, typ reflect.Type, br *compress.BitReader) (*CTickReader, error) {
	r := &CTickReader{
		tick:    0,
		br:      br,
		info:    info,
		typ:     typ,
		tickC:   nil,
		structC: nil,
	}

	return r, nil
}

func (r *CTickReader) DeltaType() reflect.Type {
	return r.typ
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
		return r.tick, delta, io.EOF
	}
	var err error
	if r.tickC == nil {
		// First next
		r.tickC, r.tick, err = compress.NewTickDecompress(r.br)
		if err != nil {
			if err == io.EOF {
				fmt.Println("Unexpected EOF reading tick")
				return r.tick, delta, io.ErrUnexpectedEOF
			} else {
				return r.tick, delta, err
			}
		}
		r.structC, delta.Pointer, err = NewStructDecompress(r.br, r.info, r.typ)
		if err != nil {
			if err == io.EOF {
				fmt.Println("Unexpected EOF reading struct")
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
					fmt.Println("Unexpected EOF reading tick")
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
					fmt.Println(r.nextTick, "Unexpected EOF reading struct")
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
