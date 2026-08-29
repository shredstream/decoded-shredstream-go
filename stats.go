package decodedshredstream

import "sync/atomic"

type counters struct {
	events          atomic.Uint64
	messages        atomic.Uint64
	bytes           atomic.Uint64
	datagrams       atomic.Uint64
	decodeErrors    atomic.Uint64
	skippedMsgType  atomic.Uint64
	queueDropped    atomic.Uint64
	reconnects      atomic.Uint64
	lastSlot        atomic.Uint64
	recvBufferBytes atomic.Uint64
}

func (c *counters) snapshot() Stats {
	return Stats{
		Events:          c.events.Load(),
		Messages:        c.messages.Load(),
		Bytes:           c.bytes.Load(),
		Datagrams:       c.datagrams.Load(),
		DecodeErrors:    c.decodeErrors.Load(),
		SkippedMsgType:  c.skippedMsgType.Load(),
		QueueDropped:    c.queueDropped.Load(),
		Reconnects:      c.reconnects.Load(),
		LastSlot:        c.lastSlot.Load(),
		RecvBufferBytes: c.recvBufferBytes.Load(),
	}
}

type Stats struct {
	Events          uint64
	Messages        uint64
	Bytes           uint64
	Datagrams       uint64
	DecodeErrors    uint64
	SkippedMsgType  uint64
	QueueDropped    uint64
	Reconnects      uint64
	LastSlot        uint64
	RecvBufferBytes uint64
}
