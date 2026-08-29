package decodedshredstream

import "errors"

var ErrMalformedMessage = errors.New("malformed transaction message")

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

func MessageBytes(tx []byte) ([]byte, error) {
	n, sz, ok := shortvec(tx, 0)
	if !ok || sz+n*64 > len(tx) {
		return nil, ErrMalformedMessage
	}
	return tx[sz+n*64:], nil
}

func Base58(b []byte) string { return base58Encode(b) }
