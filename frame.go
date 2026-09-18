package decodedshredstream

import (
	"encoding/binary"
	"time"
)

const (
	FrameMagic     uint16 = 0x5AE7
	FrameVersion   byte   = 2
	FrameHeaderLen        = 16
	MaxDatagram           = 1408
	MsgEvent       byte   = 1
	MsgTransaction byte   = 2
)

type FrameHeader struct {
	Version   byte
	MsgType   byte
	Flags     byte
	FragIndex byte
	FragCount byte
	Seq       uint64
}

type FrameError struct{ code, msg string }

func (e *FrameError) Error() string { return e.msg }

func (e *FrameError) Code() string { return e.code }

var (
	ErrTooShort           = &FrameError{"too_short", "frame shorter than the 16-byte header"}
	ErrBadMagic           = &FrameError{"bad_magic", "wrong magic — not a stream frame"}
	ErrUnsupportedVersion = &FrameError{"unsupported_version", "unknown frame version — upgrade the client"}
	ErrTruncated          = &FrameError{"truncated", "payload ended mid-field"}
	ErrBadFragment        = &FrameError{"bad_fragment", "inconsistent fragmentation fields, or a fragment lost or out of order"}
)

func ParseHeader(buf []byte) (FrameHeader, *FrameError) {
	if len(buf) < FrameHeaderLen {
		return FrameHeader{}, ErrTooShort
	}
	if binary.LittleEndian.Uint16(buf[0:2]) != FrameMagic {
		return FrameHeader{}, ErrBadMagic
	}
	if buf[2] != FrameVersion {
		return FrameHeader{}, ErrUnsupportedVersion
	}
	return FrameHeader{
		Version:   buf[2],
		MsgType:   buf[3],
		Flags:     buf[4],
		FragIndex: buf[5],
		FragCount: buf[6],
		Seq:       binary.LittleEndian.Uint64(buf[8:16]),
	}, nil
}

type PushResult int

const (
	PushMessage PushResult = iota
	PushSkipped
	PushError
)

type reassembler struct {
	buf         []byte
	fragCount   byte
	expectIndex byte
	lastSeq     uint64
	active      bool
}

func (r *reassembler) reset() {
	r.buf = r.buf[:0]
	r.fragCount = 0
	r.expectIndex = 0
	r.lastSeq = 0
	r.active = false
}

type StreamDecoder struct {
	reasm reassembler
	stats *counters
	hook  *noticeHook
}

func NewStreamDecoder() *StreamDecoder {
	return &StreamDecoder{stats: &counters{}, hook: &noticeHook{}}
}

func newStreamDecoderWith(stats *counters, hook *noticeHook) *StreamDecoder {
	return &StreamDecoder{stats: stats, hook: hook}
}

func (d *StreamDecoder) Stats() Stats { return d.stats.snapshot() }

func (d *StreamDecoder) Push(datagram []byte) (update *TransactionUpdate, res PushResult, frameErr *FrameError) {
	d.stats.datagrams.Add(1)
	d.stats.bytes.Add(uint64(len(datagram)))

	header, herr := ParseHeader(datagram)
	if herr != nil {
		d.stats.decodeErrors.Add(1)
		d.hook.fire(Notice{Kind: NoticeDecodeError})
		return nil, PushError, herr
	}

	if header.MsgType != MsgTransaction {
		d.stats.skippedMsgType.Add(1)
		return nil, PushSkipped, nil
	}

	payload := datagram[FrameHeaderLen:]

	if header.FragCount == 1 {
		d.reasm.reset()
		return d.complete(payload)
	}

	if header.FragCount == 0 || header.FragIndex >= header.FragCount {
		d.stats.decodeErrors.Add(1)
		d.hook.fire(Notice{Kind: NoticeDecodeError})
		return nil, PushError, ErrBadFragment
	}

	if header.FragIndex == 0 {
		d.reasm.reset()
		d.reasm.active = true
		d.reasm.fragCount = header.FragCount
		d.reasm.expectIndex = 1
		d.reasm.lastSeq = header.Seq
		d.reasm.buf = append(d.reasm.buf, payload...)
		return nil, PushSkipped, nil
	}

	if !d.reasm.active ||
		d.reasm.fragCount != header.FragCount ||
		header.FragIndex != d.reasm.expectIndex ||
		header.Seq != d.reasm.lastSeq+1 {
		d.reasm.reset()
		d.stats.decodeErrors.Add(1)
		d.hook.fire(Notice{Kind: NoticeDecodeError})
		return nil, PushError, ErrBadFragment
	}
	d.reasm.buf = append(d.reasm.buf, payload...)
	d.reasm.lastSeq = header.Seq
	d.reasm.expectIndex++

	if header.FragIndex+1 == header.FragCount {
		update, res, err := d.complete(d.reasm.buf)
		d.reasm.reset()
		return update, res, err
	}
	return nil, PushSkipped, nil
}

func (d *StreamDecoder) complete(payload []byte) (*TransactionUpdate, PushResult, *FrameError) {
	if len(payload) < 8 {
		d.stats.decodeErrors.Add(1)
		d.hook.fire(Notice{Kind: NoticeDecodeError})
		return nil, PushError, ErrTruncated
	}

	tx := make([]byte, len(payload)-8)
	copy(tx, payload[8:])
	u := &TransactionUpdate{
		Slot:       binary.LittleEndian.Uint64(payload[0:8]),
		ReceivedAt: time.Now(),
		bytes:      tx,
	}
	d.stats.messages.Add(1)
	d.stats.events.Add(1)
	d.stats.lastSlot.Store(u.Slot)
	return u, PushMessage, nil
}
