package decodedshredstream

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/ipv4"
	"golang.org/x/net/ipv6"
)

const readBatchN = 32

type UDPConfig struct {
	Port            int
	Host            string
	RecvBufferBytes int
	QueueCapacity   int
}

type batchReader interface {
	ReadBatch(ms []ipv4.Message, flags int) (int, error)
}

type UDPClient struct {
	conn    *net.UDPConn
	batch   batchReader
	stats   *counters
	hook    *noticeHook
	pending []Notice // fired on OnNotice registration (e.g. buffer clamp)
	pendMu  sync.Mutex

	queueCap int
	closed   atomic.Bool
	running  atomic.Bool

	updatesOnce sync.Once
	updatesCh   chan *TransactionUpdate
	errMu       sync.Mutex
	err         error
	errReady    chan struct{}
}

func NewUDP(cfg UDPConfig) (*UDPClient, error) {
	host := cfg.Host
	if host == "" {
		host = "0.0.0.0"
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return nil, fmt.Errorf("decodedshredstream: invalid host %q", cfg.Host)
	}
	recvBuf := cfg.RecvBufferBytes
	if recvBuf == 0 {
		recvBuf = 64 << 20
	}
	queueCap := cfg.QueueCapacity
	if queueCap <= 0 {
		queueCap = 8192
	}

	network := "udp6"
	if ip.To4() != nil {
		network = "udp4"
	}
	conn, err := net.ListenUDP(network, &net.UDPAddr{IP: ip, Port: cfg.Port})
	if err != nil {
		return nil, fmt.Errorf("decodedshredstream: bind %s:%d: %w", host, cfg.Port, err)
	}
	c := &UDPClient{
		conn:     conn,
		stats:    &counters{},
		hook:     &noticeHook{},
		queueCap: queueCap,
		errReady: make(chan struct{}),
	}
	if ip.To4() != nil {
		c.batch = ipv4.NewPacketConn(conn)
	} else {
		c.batch = ipv6.NewPacketConn(conn)
	}

	_ = conn.SetReadBuffer(recvBuf)
	if effective := recvBufferSize(conn); effective >= 0 {
		c.stats.recvBufferBytes.Store(uint64(effective))
		if effective < recvBuf {
			c.pending = append(c.pending, Notice{
				Kind:      NoticeRecvBufferClamped,
				Requested: recvBuf,
				Effective: effective,
			})
		}
	}
	return c, nil
}

func (c *UDPClient) LocalAddr() *net.UDPAddr {
	return c.conn.LocalAddr().(*net.UDPAddr)
}

func (c *UDPClient) OnNotice(fn func(Notice)) {
	c.hook.set(fn)
	c.pendMu.Lock()
	pending := c.pending
	c.pending = nil
	c.pendMu.Unlock()
	for _, n := range pending {
		c.hook.fire(n)
	}
}

func (c *UDPClient) Stats() Stats { return c.stats.snapshot() }

func (c *UDPClient) Run(ctx context.Context, handler func(*TransactionUpdate)) error {
	if handler == nil {
		return errors.New("decodedshredstream: nil handler")
	}
	if !c.running.CompareAndSwap(false, true) {
		return errors.New("decodedshredstream: Run/Updates already active")
	}
	defer c.running.Store(false)

	_ = c.conn.SetReadDeadline(time.Time{})

	watchDone := make(chan struct{})
	defer close(watchDone)
	go func() {
		select {
		case <-ctx.Done():
			_ = c.conn.SetReadDeadline(time.Unix(0, 1))
		case <-watchDone:
		}
	}()

	decoder := newStreamDecoderWith(c.stats, c.hook)
	msgs := make([]ipv4.Message, readBatchN)
	for i := range msgs {
		msgs[i].Buffers = [][]byte{make([]byte, MaxDatagram+64)}
	}

	for {
		n, err := c.batch.ReadBatch(msgs, 0)
		if err != nil {
			if c.closed.Load() {
				return ErrClosed
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(err, os.ErrDeadlineExceeded) {
				continue
			}
			return fmt.Errorf("decodedshredstream: udp read: %w", err)
		}
		for i := 0; i < n; i++ {
			update, res, _ := decoder.Push(msgs[i].Buffers[0][:msgs[i].N])
			if res == PushMessage {
				handler(update)
			}
		}
	}
}

func (c *UDPClient) Updates() <-chan *TransactionUpdate {
	c.updatesOnce.Do(func() {
		ch := make(chan *TransactionUpdate, c.queueCap)
		c.updatesCh = ch
		go func() {
			defer close(ch)
			defer close(c.errReady)
			defer func() {
				if r := recover(); r != nil {
					c.errMu.Lock()
					c.err = fmt.Errorf("decodedshredstream: receive callback panicked: %v", r)
					c.errMu.Unlock()
				}
			}()
			err := c.Run(context.Background(), func(u *TransactionUpdate) {
				pushDropOldest(ch, u, &c.stats.queueDropped)
			})
			c.errMu.Lock()
			c.err = err
			c.errMu.Unlock()
		}()
	})
	return c.updatesCh
}

func (c *UDPClient) Err() error {
	select {
	case <-c.errReady:
	default:
		return nil
	}
	c.errMu.Lock()
	defer c.errMu.Unlock()
	return c.err
}

func (c *UDPClient) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	return c.conn.Close()
}

func pushDropOldest(ch chan *TransactionUpdate, u *TransactionUpdate, dropped *atomic.Uint64) {
	select {
	case ch <- u:
		return
	default:
	}
	select {
	case <-ch:
		dropped.Add(1)
	default:
	}
	select {
	case ch <- u:
	default:
		dropped.Add(1)
	}
}
