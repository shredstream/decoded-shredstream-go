package decodedshredstream

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/shredstream/decoded-shredstream-go/pb"
)

const DefaultGRPCPort = 9991

type AuthStyle int

const (
	AuthBearer AuthStyle = iota
	AuthXToken
)

type ReconnectPolicy struct {
	Initial    time.Duration
	Max        time.Duration
	Multiplier float64
	ResetAfter time.Duration
}

func (p ReconnectPolicy) withDefaults() ReconnectPolicy {
	if p.Initial <= 0 {
		p.Initial = 100 * time.Millisecond
	}
	if p.Max <= 0 {
		p.Max = 5 * time.Second
	}
	if p.Multiplier <= 0 {
		p.Multiplier = 2.0
	}
	if p.ResetAfter <= 0 {
		p.ResetAfter = 30 * time.Second
	}
	return p
}

type GRPCConfig struct {
	Endpoint       string
	Token          string
	AuthStyle      AuthStyle
	Filters        Filters
	Reconnect      ReconnectPolicy
	ConnectTimeout time.Duration
}

type subscribeStream = grpc.BidiStreamingClient[pb.SubscribeBinaryTransactionsRequest, pb.SubscribeBinaryTransactionsResponse]

type GRPCClient struct {
	client         pb.DecodedShredStreamServiceClient
	conn           *grpc.ClientConn
	md             metadata.MD
	connectTimeout time.Duration
	reconnect      ReconnectPolicy

	stats *counters
	hook  *noticeHook

	lifeCtx    context.Context
	lifeCancel context.CancelFunc

	mu           sync.Mutex // guards filters, stream, streamCancel and sends
	filters      Filters
	stream       subscribeStream
	streamCancel context.CancelFunc

	attempt       int
	everConnected bool
	connectedAt   time.Time
	lastDataLoss  time.Time

	closed  atomic.Bool
	running atomic.Bool

	updatesOnce sync.Once
	updatesCh   chan *TransactionUpdate
	errMu       sync.Mutex
	err         error
	errReady    chan struct{}
}

func normalizeGRPCEndpoint(endpoint string) (string, error) {
	e := strings.TrimSpace(endpoint)
	e = strings.TrimPrefix(e, "http://")
	if e == "" || strings.Contains(e, "/") || strings.HasPrefix(endpoint, "https://") {
		return "", fmt.Errorf("decodedshredstream: invalid endpoint %q", endpoint)
	}
	hasPort := false
	if i := strings.LastIndexByte(e, ':'); i >= 0 {
		port := e[i+1:]
		allDigits := port != ""
		for _, c := range port {
			if c < '0' || c > '9' {
				allDigits = false
				break
			}
		}
		hasPort = allDigits && (!strings.Contains(e, "]") || i > strings.LastIndexByte(e, ']'))
	}
	if !hasPort {
		e = e + ":" + strconv.Itoa(DefaultGRPCPort)
	}
	return e, nil
}

func parseLagged(message string) uint64 {
	rest, ok := strings.CutPrefix(message, "client lagged:")
	if !ok {
		if _, r, ok2 := strings.Cut(message, ":"); ok2 {
			rest = r
		} else {
			return 0
		}
	}
	rest = strings.TrimSpace(rest)
	if i := strings.IndexByte(rest, ' '); i >= 0 {
		rest = rest[:i]
	}
	n, err := strconv.ParseUint(rest, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

func NewGRPC(cfg GRPCConfig) (*GRPCClient, error) {
	if err := cfg.Filters.Validate(); err != nil {
		return nil, err
	}
	target, err := normalizeGRPCEndpoint(cfg.Endpoint)
	if err != nil {
		return nil, err
	}
	var md metadata.MD
	switch cfg.AuthStyle {
	case AuthBearer:
		md = metadata.Pairs("authorization", "Bearer "+cfg.Token)
	case AuthXToken:
		md = metadata.Pairs("x-token", cfg.Token)
	default:
		return nil, fmt.Errorf("decodedshredstream: unknown auth style %d", cfg.AuthStyle)
	}
	connectTimeout := cfg.ConnectTimeout
	if connectTimeout <= 0 {
		connectTimeout = 5 * time.Second
	}

	conn, err := grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()), // h2c by design
		grpc.WithInitialWindowSize(4<<20),
		grpc.WithInitialConnWindowSize(16<<20),
	)
	if err != nil {
		return nil, fmt.Errorf("decodedshredstream: dial %s: %w", target, err)
	}

	lifeCtx, lifeCancel := context.WithCancel(context.Background())
	c := &GRPCClient{
		client:         pb.NewDecodedShredStreamServiceClient(conn),
		conn:           conn,
		md:             md,
		connectTimeout: connectTimeout,
		reconnect:      cfg.Reconnect.withDefaults(),
		stats:          &counters{},
		hook:           &noticeHook{},
		lifeCtx:        lifeCtx,
		lifeCancel:     lifeCancel,
		filters:        cfg.Filters.clone(),
		errReady:       make(chan struct{}),
	}

	if err := c.connectOnce(); err != nil {
		_ = c.Close()
		switch classify(err) {
		case classTerminal:
			return nil, fmt.Errorf("decodedshredstream: connect: %w", mapTerminal(err))
		default:
			return nil, fmt.Errorf("decodedshredstream: connect %s: %w", target, err)
		}
	}
	return c, nil
}

func (c *GRPCClient) connectOnce() error {
	streamCtx, cancel := context.WithCancel(metadata.NewOutgoingContext(c.lifeCtx, c.md))
	stream, err := c.client.SubscribeDecodedTransactions(streamCtx)
	if err != nil {
		cancel()
		return err
	}
	_ = stream.Send(filtersToProto(c.currentFilters()))

	type headerRes struct {
		md  metadata.MD
		err error
	}
	headerCh := make(chan headerRes, 1)
	go func() {
		md, herr := stream.Header()
		headerCh <- headerRes{md, herr}
	}()
	timer := time.NewTimer(c.connectTimeout)
	defer timer.Stop()
	var hdr headerRes
	select {
	case hdr = <-headerCh:
	case <-timer.C:
		cancel()
		return status.Error(codes.Unavailable, "connection timed out")
	}
	if hdr.err != nil {
		cancel()
		return hdr.err
	}
	if len(hdr.md.Get("content-type")) == 0 || stream.Context().Err() != nil {
		_, rerr := stream.Recv()
		cancel()
		if rerr == nil {
			rerr = status.Error(codes.Unavailable, "stream ended during connect")
		}
		return rerr
	}

	c.mu.Lock()
	if c.streamCancel != nil {
		c.streamCancel()
	}
	c.stream = stream
	c.streamCancel = cancel
	c.mu.Unlock()

	if c.everConnected {
		c.stats.reconnects.Add(1)
		c.hook.fire(Notice{Kind: NoticeReconnected})
	}
	c.everConnected = true
	c.connectedAt = time.Now()
	return nil
}

func (c *GRPCClient) currentFilters() Filters {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.filters
}

func (c *GRPCClient) currentStream() subscribeStream {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stream
}

func (c *GRPCClient) dropStream() {
	c.mu.Lock()
	if c.streamCancel != nil {
		c.streamCancel()
	}
	c.stream = nil
	c.streamCancel = nil
	c.mu.Unlock()
}

func filtersToProto(filters Filters) *pb.SubscribeBinaryTransactionsRequest {
	transactions := make(map[string]*pb.SubscribeRequestFilterBinaryTransactions, len(filters))
	for name, f := range filters {
		transactions[name] = &pb.SubscribeRequestFilterBinaryTransactions{
			AccountInclude:  f.Include,
			AccountExclude:  f.Exclude,
			AccountRequired: f.Required,
		}
	}
	return &pb.SubscribeBinaryTransactionsRequest{Transactions: transactions}
}

func (c *GRPCClient) backoffDelay() time.Duration {
	c.attempt++
	exp := c.reconnect.Initial.Seconds()
	for i := 1; i < c.attempt; i++ {
		exp *= c.reconnect.Multiplier
		if exp >= c.reconnect.Max.Seconds() {
			break
		}
	}
	capped := min(exp, c.reconnect.Max.Seconds())
	return time.Duration(capped * rand.Float64() * float64(time.Second))
}

func (c *GRPCClient) Run(ctx context.Context, handler func(*TransactionUpdate)) error {
	if handler == nil {
		return errors.New("decodedshredstream: nil handler")
	}
	if !c.running.CompareAndSwap(false, true) {
		return errors.New("decodedshredstream: Run/Updates already active")
	}
	defer c.running.Store(false)

	watchDone := make(chan struct{})
	defer close(watchDone)
	go func() {
		select {
		case <-ctx.Done():
			c.dropStream()
		case <-c.lifeCtx.Done():
		case <-watchDone:
		}
	}()

	for {
		if err := c.checkDone(ctx); err != nil {
			return err
		}
		stream := c.currentStream()
		if stream == nil {
			delay := c.backoffDelay()
			c.hook.fire(Notice{Kind: NoticeReconnecting, Attempt: c.attempt, Delay: delay})
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-c.lifeCtx.Done():
				timer.Stop()
				return ErrClosed
			}
			if err := c.connectOnce(); err != nil {
				if done := c.checkDone(ctx); done != nil {
					return done
				}
				if classify(err) == classTerminal {
					return mapTerminal(err)
				}
				continue // stays disconnected; next backoff
			}
			continue
		}

		resp, err := stream.Recv()
		if err == nil {
			if time.Since(c.connectedAt) >= c.reconnect.ResetAfter {
				c.attempt = 0
			}
			if u := c.toUpdate(resp); u != nil {
				handler(u)
			}
			continue
		}

		if done := c.checkDone(ctx); done != nil {
			return done
		}
		switch classify(err) {
		case classTerminal:
			c.dropStream()
			return mapTerminal(err)
		case classDataLoss:
			recent := !c.lastDataLoss.IsZero() && time.Since(c.lastDataLoss) < 10*time.Second
			c.lastDataLoss = time.Now()
			c.dropStream()
			if !recent {
				_ = c.connectOnce() // on failure the loop backs off
			}
		default: // recoverable
			c.dropStream()
		}
	}
}

func (c *GRPCClient) checkDone(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if c.closed.Load() {
		return ErrClosed
	}
	return nil
}

func (c *GRPCClient) toUpdate(resp *pb.SubscribeBinaryTransactionsResponse) *TransactionUpdate {
	wrapper := resp.GetTransaction()
	tx := wrapper.GetTransaction()
	if tx == nil || len(tx.GetBinaryTransaction()) == 0 {
		c.stats.decodeErrors.Add(1)
		c.hook.fire(Notice{Kind: NoticeDecodeError})
		return nil
	}
	raw := tx.GetBinaryTransaction()
	c.stats.messages.Add(1)
	c.stats.events.Add(1)
	c.stats.bytes.Add(uint64(len(raw)))
	c.stats.lastSlot.Store(wrapper.GetSlot())

	u := &TransactionUpdate{
		Slot:       wrapper.GetSlot(),
		Filters:    resp.GetFilters(),
		ReceivedAt: time.Now(),
		bytes:      raw,
		protoSigs:  tx.GetSignatures(),
	}
	if ts := resp.GetCreatedAt(); ts != nil {
		u.CreatedAt = ts.AsTime()
	}
	return u
}

func (c *GRPCClient) UpdateFilters(filters Filters) error {
	if err := filters.Validate(); err != nil {
		return err
	}
	c.mu.Lock()
	c.filters = filters.clone()
	stream := c.stream
	proto := filtersToProto(c.filters)
	c.mu.Unlock()
	if stream != nil {
		_ = stream.Send(proto)
	}
	return nil
}

func (c *GRPCClient) OnNotice(fn func(Notice)) { c.hook.set(fn) }

func (c *GRPCClient) Stats() Stats { return c.stats.snapshot() }

func (c *GRPCClient) Updates() <-chan *TransactionUpdate {
	c.updatesOnce.Do(func() {
		ch := make(chan *TransactionUpdate, 8192)
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

func (c *GRPCClient) Err() error {
	select {
	case <-c.errReady:
	default:
		return nil
	}
	c.errMu.Lock()
	defer c.errMu.Unlock()
	return c.err
}

func (c *GRPCClient) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	c.lifeCancel()
	return c.conn.Close()
}

type errClass int

const (
	classRecoverable errClass = iota
	classTerminal
	classDataLoss
)

func classify(err error) errClass {
	switch status.Code(err) {
	case codes.Unauthenticated, codes.PermissionDenied, codes.InvalidArgument:
		return classTerminal
	case codes.DataLoss:
		return classDataLoss
	default:
		return classRecoverable
	}
}

func mapTerminal(err error) error {
	s := status.Convert(err)
	switch s.Code() {
	case codes.Unauthenticated:
		return ErrAuthRefused
	case codes.PermissionDenied:
		return ErrKicked
	case codes.InvalidArgument:
		return invalidFilterf("%s", s.Message())
	}
	return err
}
