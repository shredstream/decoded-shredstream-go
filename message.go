package decodedshredstream

import (
	"encoding/binary"
	"errors"
)

var ErrMalformedMessage = errors.New("malformed transaction message")

const (
	txV1Marker      = 0x81
	v1MinMessageLen = 42
)

func v1MessageLen(tx []byte) (int, bool) {
	if len(tx) < v1MinMessageLen {
		return 0, false
	}
	n := len(tx) - 64*int(tx[1])
	if n < v1MinMessageLen {
		return 0, false
	}
	return n, true
}

type CompiledInstruction struct {
	ProgramIndex   uint8
	AccountIndices []byte
	Data           []byte
}

type AddressTableLookup struct {
	TableAddress    []byte
	WritableIndexes []byte
	ReadonlyIndexes []byte
}

type MessageHeader struct {
	NumRequiredSignatures uint8
	NumReadonlySigned     uint8
	NumReadonlyUnsigned   uint8
}

type CompiledMessage struct {
	Version             int
	Header              MessageHeader
	StaticAccounts      [][]byte
	LifetimeToken       []byte
	Instructions        []CompiledInstruction
	AddressTableLookups []AddressTableLookup
}

func shortvec(b []byte, off int) (value, size int, ok bool) {
	shift, o := uint(0), off
	for {
		if o >= len(b) {
			return 0, 0, false
		}
		c := b[o]
		o++
		value |= int(c&0x7f) << shift
		if c&0x80 == 0 {
			break
		}
		shift += 7
		if shift > 21 {
			return 0, 0, false
		}
	}
	return value, o - off, true
}

func DecodeMessage(buf []byte) (*CompiledMessage, error) {
	if len(buf) == 0 {
		return nil, ErrMalformedMessage
	}
	if buf[0] == txV1Marker {
		return decodeMessageV1(buf)
	}
	o := 0

	version := -1
	if buf[0]&0x80 != 0 {
		version = int(buf[0] & 0x7f)
		o = 1
	}

	if o+3 > len(buf) {
		return nil, ErrMalformedMessage
	}
	m := &CompiledMessage{
		Version: version,
		Header: MessageHeader{
			NumRequiredSignatures: buf[o],
			NumReadonlySigned:     buf[o+1],
			NumReadonlyUnsigned:   buf[o+2],
		},
	}
	o += 3

	n, sz, ok := shortvec(buf, o)
	if !ok {
		return nil, ErrMalformedMessage
	}
	o += sz
	if o+n*32 > len(buf) {
		return nil, ErrMalformedMessage
	}
	m.StaticAccounts = make([][]byte, n)
	for i := 0; i < n; i++ {
		m.StaticAccounts[i] = buf[o : o+32]
		o += 32
	}

	if o+32 > len(buf) {
		return nil, ErrMalformedMessage
	}
	m.LifetimeToken = buf[o : o+32]
	o += 32

	n, sz, ok = shortvec(buf, o)
	if !ok {
		return nil, ErrMalformedMessage
	}
	o += sz
	m.Instructions = make([]CompiledInstruction, n)
	for i := 0; i < n; i++ {
		if o >= len(buf) {
			return nil, ErrMalformedMessage
		}
		prog := buf[o]
		o++
		na, sz, ok := shortvec(buf, o)
		if !ok || o+sz+na > len(buf) {
			return nil, ErrMalformedMessage
		}
		o += sz
		idx := buf[o : o+na]
		o += na
		nd, sz, ok := shortvec(buf, o)
		if !ok || o+sz+nd > len(buf) {
			return nil, ErrMalformedMessage
		}
		o += sz
		data := buf[o : o+nd]
		o += nd
		m.Instructions[i] = CompiledInstruction{ProgramIndex: prog, AccountIndices: idx, Data: data}
	}

	if version >= 0 {
		n, sz, ok = shortvec(buf, o)
		if !ok {
			return nil, ErrMalformedMessage
		}
		o += sz
		m.AddressTableLookups = make([]AddressTableLookup, n)
		for i := 0; i < n; i++ {
			if o+32 > len(buf) {
				return nil, ErrMalformedMessage
			}
			key := buf[o : o+32]
			o += 32
			nw, sz, ok := shortvec(buf, o)
			if !ok || o+sz+nw > len(buf) {
				return nil, ErrMalformedMessage
			}
			o += sz
			w := buf[o : o+nw]
			o += nw
			nr, sz, ok := shortvec(buf, o)
			if !ok || o+sz+nr > len(buf) {
				return nil, ErrMalformedMessage
			}
			o += sz
			r := buf[o : o+nr]
			o += nr
			m.AddressTableLookups[i] = AddressTableLookup{
				TableAddress:    key,
				WritableIndexes: w,
				ReadonlyIndexes: r,
			}
		}
	}

	return m, nil
}

func decodeMessageV1(buf []byte) (*CompiledMessage, error) {
	if len(buf) < v1MinMessageLen {
		return nil, ErrMalformedMessage
	}
	m := &CompiledMessage{
		Version: 1,
		Header: MessageHeader{
			NumRequiredSignatures: buf[1],
			NumReadonlySigned:     buf[2],
			NumReadonlyUnsigned:   buf[3],
		},
		LifetimeToken: buf[8:40],
	}
	mask := binary.LittleEndian.Uint32(buf[4:8])
	nIx := int(buf[40])
	nAddr := int(buf[41])
	o := 42

	if o+nAddr*32 > len(buf) {
		return nil, ErrMalformedMessage
	}
	m.StaticAccounts = make([][]byte, nAddr)
	for i := range m.StaticAccounts {
		m.StaticAccounts[i] = buf[o : o+32]
		o += 32
	}

	if mask&0b11 == 0b11 {
		o += 8
	}
	for bit := 2; bit <= 4; bit++ {
		if mask&(1<<bit) != 0 {
			o += 4
		}
	}

	if o+nIx*4 > len(buf) {
		return nil, ErrMalformedMessage
	}
	p := o + nIx*4
	m.Instructions = make([]CompiledInstruction, nIx)
	for i := range m.Instructions {
		na := int(buf[o+1])
		nd := int(binary.LittleEndian.Uint16(buf[o+2 : o+4]))
		if p+na+nd > len(buf) {
			return nil, ErrMalformedMessage
		}
		m.Instructions[i] = CompiledInstruction{
			ProgramIndex:   buf[o],
			AccountIndices: buf[p : p+na],
			Data:           buf[p+na : p+na+nd],
		}
		o += 4
		p += na + nd
	}
	return m, nil
}

func MessageBytes(tx []byte) ([]byte, error) {
	if len(tx) > 0 && tx[0] == txV1Marker {
		n, ok := v1MessageLen(tx)
		if !ok {
			return nil, ErrMalformedMessage
		}
		return tx[:n], nil
	}
	n, sz, ok := shortvec(tx, 0)
	if !ok || sz+n*64 > len(tx) {
		return nil, ErrMalformedMessage
	}
	return tx[sz+n*64:], nil
}

func Base58(b []byte) string { return base58Encode(b) }
