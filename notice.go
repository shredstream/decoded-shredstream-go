package decodedshredstream

import (
	"sync"
	"time"
)

type NoticeKind int

const (
	NoticeDecodeError NoticeKind = iota + 1
	NoticeRecvBufferClamped
	NoticeReconnecting
	NoticeReconnected
)

func (k NoticeKind) String() string {
	switch k {
	case NoticeDecodeError:
		return "decode_error"
	case NoticeRecvBufferClamped:
		return "recv_buffer_clamped"
	case NoticeReconnecting:
		return "reconnecting"
	case NoticeReconnected:
		return "reconnected"
	}
	return "unknown"
}

type Notice struct {
	Kind NoticeKind

	Requested int
	Effective int

	Attempt int
	Delay   time.Duration
}

type noticeHook struct {
	mu sync.RWMutex
	fn func(Notice)
}

func (h *noticeHook) set(fn func(Notice)) {
	h.mu.Lock()
	h.fn = fn
	h.mu.Unlock()
}

func (h *noticeHook) fire(n Notice) {
	h.mu.RLock()
	fn := h.fn
	h.mu.RUnlock()
	if fn != nil {
		defer func() { _ = recover() }()
		fn(n)
	}
}
