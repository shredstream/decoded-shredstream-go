package decodedshredstream

import (
	"sync"
	"time"
)

type Signature [64]byte

func (s Signature) Base58() string { return base58Encode(s[:]) }

func (s Signature) String() string { return s.Base58() }

type TransactionUpdate struct {
	Slot       uint64
	Filters    []string
	CreatedAt  time.Time
	ReceivedAt time.Time

	bytes     []byte
	protoSigs [][]byte // signatures provided by the gRPC message, if any

	sigsOnce sync.Once
	sigs     []Signature
}

func (u *TransactionUpdate) Bytes() []byte { return u.bytes }

func shortvecLen(b []byte) (val int, n int, ok bool) {
	shift := uint(0)
	for i := 0; i < len(b) && i < 3; i++ {
		val |= int(b[i]&0x7F) << shift
		if b[i]&0x80 == 0 {
			return val, i + 1, true
		}
		shift += 7
	}
	return 0, 0, false
}

func (u *TransactionUpdate) Signature() (Signature, bool) {
	var sig Signature
	if len(u.protoSigs) > 0 && len(u.protoSigs[0]) == 64 {
		copy(sig[:], u.protoSigs[0])
		return sig, true
	}
	if len(u.bytes) > 0 && u.bytes[0] == txV1Marker {
		start, ok := v1MessageLen(u.bytes)
		if !ok || u.bytes[1] < 1 {
			return Signature{}, false
		}
		copy(sig[:], u.bytes[start:start+64])
		return sig, true
	}
	count, prefix, ok := shortvecLen(u.bytes)
	if !ok || count < 1 || len(u.bytes) < prefix+64 {
		return Signature{}, false
	}
	copy(sig[:], u.bytes[prefix:prefix+64])
	return sig, true
}

func (u *TransactionUpdate) Signatures() []Signature {
	u.sigsOnce.Do(func() {
		if len(u.protoSigs) > 0 {
			sigs := make([]Signature, 0, len(u.protoSigs))
			for _, raw := range u.protoSigs {
				if len(raw) != 64 {
					return
				}
				var s Signature
				copy(s[:], raw)
				sigs = append(sigs, s)
			}
			u.sigs = sigs
			return
		}
		if len(u.bytes) > 0 && u.bytes[0] == txV1Marker {
			start, ok := v1MessageLen(u.bytes)
			if !ok || u.bytes[1] < 1 {
				return
			}
			sigs := make([]Signature, u.bytes[1])
			for i := range sigs {
				copy(sigs[i][:], u.bytes[start+i*64:])
			}
			u.sigs = sigs
			return
		}
		count, prefix, ok := shortvecLen(u.bytes)
		if !ok || count < 1 || len(u.bytes) < prefix+count*64 {
			return
		}
		sigs := make([]Signature, count)
		for i := 0; i < count; i++ {
			copy(sigs[i][:], u.bytes[prefix+i*64:])
		}
		u.sigs = sigs
	})
	return u.sigs
}

func (u *TransactionUpdate) Parse() (*CompiledMessage, error) {
	msg, err := MessageBytes(u.Bytes())
	if err != nil {
		return nil, err
	}
	return DecodeMessage(msg)
}
