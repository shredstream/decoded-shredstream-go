# API reference — Decoded ShredStream Go client

Complete surface of the client. Start with the [README](../README.md) to
receive your first transactions.

## 🧩 Data model

Every transaction, on either transport, arrives as a `*TransactionUpdate`.

| Field / method | Type | Meaning |
|---|---|---|
| `Slot` | `uint64` | Solana slot the transaction landed in. |
| `Filters` | `[]string` | Names of the server-side filters that matched. Empty in UDP mode. |
| `CreatedAt` | `time.Time` | Server emission timestamp, from the server's wall clock — not a latency reference. Set in gRPC mode; zero in UDP mode. |
| `ReceivedAt` | `time.Time` | Local timestamp taken when the message was received. |
| `Bytes()` | `[]byte` | The transaction in the standard Solana wire format. |
| `Signature()` | `(Signature, bool)` | First signature (the fee payer's). |
| `Signatures()` | `[]Signature` | All signatures, index 0 being the fee payer's. |
| `Parse()` | `(*CompiledMessage, error)` | Decodes the message: accounts, instructions, lifetime token. |

`Bytes()` returns the update's own buffer: do not modify the slice.

`Signature` is `[64]byte` with a `Base58() string` method (`String()` renders
the same). `Signature()` returns `false`, and `Signatures()` returns `nil`,
when the bytes are malformed.

A `TransactionUpdate` is safe for concurrent reads.

## 🔐 Authentication

Authentication applies to gRPC only. The token travels in the stream's
metadata, in one of two equivalent forms:

| `AuthStyle` | Metadata sent |
|---|---|
| `AuthBearer` (default, zero value) | `authorization: Bearer <token>` |
| `AuthXToken` | `x-token: <token>` |

```go
client, err := decodedshredstream.NewGRPC(decodedshredstream.GRPCConfig{
	Endpoint:  endpoint,
	Token:     token,
	AuthStyle: decodedshredstream.AuthXToken,
	Filters:   decodedshredstream.Filters{"all": decodedshredstream.FilterAll()},
})
```

## 📚 Types and methods

### Package `decodedshredstream`

#### Constructors

```go
func NewUDP(cfg UDPConfig) (*UDPClient, error)
```
Binds the local address and port from `cfg` and returns a client ready to
receive. Fails when `Host` is not a valid IP address or when the port cannot
be bound.

```go
func NewGRPC(cfg GRPCConfig) (*GRPCClient, error)
```
Validates `cfg`, opens the connection and subscribes with the configured
filters. The returned client must be closed with `Close`.

#### `type UDPConfig`

```go
type UDPConfig struct {
	Port            int    // local UDP port to bind
	Host            string // local address to bind; default "0.0.0.0"
	RecvBufferBytes int    // receive buffer to request, in bytes; default 64 MiB
	QueueCapacity   int    // capacity of the Updates channel; default 8192
}
```

The default `Host`, `0.0.0.0`, listens on IPv4; pass `"::"` to listen on IPv6.

#### `type UDPClient`

```go
func (c *UDPClient) Run(ctx context.Context, handler func(*TransactionUpdate)) error
```
Reads the socket on the calling goroutine and calls `handler` there for every
transaction. Blocks until `ctx` is cancelled (returns `ctx.Err()`), `Close`
is called (`ErrClosed`) or the socket fails (the wrapped I/O error).

```go
func (c *UDPClient) Updates() <-chan *TransactionUpdate
```
Starts the receive loop on its own goroutine and returns the buffered channel
it feeds; repeated calls return the same channel. Drops the oldest queued
update on overflow. Closed when the stream ends.

```go
func (c *UDPClient) Err() error
```
The error that ended the `Updates` stream, or `nil` while it is still
running.

```go
func (c *UDPClient) Stats() Stats
func (c *UDPClient) OnNotice(fn func(Notice))
func (c *UDPClient) LocalAddr() *net.UDPAddr
func (c *UDPClient) Close() error
```
`OnNotice` replaces any previous callback and immediately delivers notices
recorded before registration. `LocalAddr` resolves the actual port when
`Port` was left at 0. `Close` is idempotent; updates already queued on the
`Updates` channel stay readable.

#### `type GRPCConfig`

```go
type GRPCConfig struct {
	Endpoint       string          // "host" or "host:port"; port defaults to DefaultGRPCPort
	Token          string          // access token
	AuthStyle      AuthStyle       // metadata style; default AuthBearer
	Filters        Filters         // named server-side filters; must not be empty
	Reconnect      ReconnectPolicy // zero fields take their defaults
	ConnectTimeout time.Duration   // default 5s
}
```
IPv6 literals must be bracketed, as in `[::1]:9991`.

#### `type GRPCClient`

```go
func (c *GRPCClient) Run(ctx context.Context, handler func(*TransactionUpdate)) error
```
Drives the stream and calls `handler` on the receive goroutine. Blocks until
the stream ends: `ctx.Err()`, `ErrClosed`, `ErrAuthRefused`, `ErrKicked` or
an error matching `ErrInvalidFilter`. Recoverable interruptions are
reconnected internally and do not surface here.

```go
func (c *GRPCClient) Updates() <-chan *TransactionUpdate
func (c *GRPCClient) Err() error
func (c *GRPCClient) UpdateFilters(filters Filters) error
func (c *GRPCClient) Stats() Stats
func (c *GRPCClient) OnNotice(fn func(Notice))
func (c *GRPCClient) Close() error
```
`Updates` holds 8192 updates and drops the oldest on overflow.
`UpdateFilters` validates, then replaces the whole map on the live stream;
an invalid map leaves the previous one in force and returns an error
matching `ErrInvalidFilter`. `Close` is idempotent.

#### `type CompiledMessage`

```go
type CompiledMessage struct {
	Version             int    // -1 for a legacy message
	Header              MessageHeader
	StaticAccounts      [][]byte // 32 bytes each, wire order
	LifetimeToken       []byte   // recent blockhash, or a nonce
	Instructions        []CompiledInstruction
	AddressTableLookups []AddressTableLookup // nil on legacy
}

type CompiledInstruction struct {
	ProgramIndex   uint8  // index into StaticAccounts
	AccountIndices []byte
	Data           []byte
}

type AddressTableLookup struct {
	TableAddress    []byte // 32-byte address of the lookup table account
	WritableIndexes []byte
	ReadonlyIndexes []byte
}

func DecodeMessage(buf []byte) (*CompiledMessage, error)
func MessageBytes(tx []byte) ([]byte, error)
func Base58(b []byte) string
```
`DecodeMessage` is the decoder `Parse` uses, exported for message bytes
obtained elsewhere; `MessageBytes` splits a serialized transaction. Errors wrap
`ErrMalformedMessage`, testable with `errors.Is`.

#### Filters

```go
type Filter struct {
	Include  []string
	Exclude  []string
	Required []string
}

type Filters map[string]Filter

func FilterAll() Filter
func (m Filters) Validate() error
```

#### Authentication and reconnection

```go
type AuthStyle int

const (
	AuthBearer AuthStyle = iota // authorization: Bearer <token>
	AuthXToken                  // x-token: <token>
)

type ReconnectPolicy struct {
	Initial    time.Duration // default 100ms
	Max        time.Duration // default 5s
	Multiplier float64       // default 2
	ResetAfter time.Duration // default 30s
}

const DefaultGRPCPort = 9991
```

#### Errors

```go
var ErrAuthRefused   error
var ErrKicked        error
var ErrInvalidFilter error
var ErrClosed        error
```

#### Notices and statistics

```go
type NoticeKind int

const (
	NoticeDecodeError NoticeKind = iota + 1
	NoticeRecvBufferClamped
	NoticeReconnecting
	NoticeReconnected
)

func (k NoticeKind) String() string

type Notice struct {
	Kind      NoticeKind
	Requested int
	Effective int
	Attempt   int
	Delay     time.Duration
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
```

#### Frame decoding

The UDP framing is exposed so you can decode datagrams read from a socket you
manage yourself. One datagram is one frame: a 16-byte header followed by the
payload, all integer fields little-endian, 1408 bytes at most.

| Offset | Size | Field | Value |
|---|---|---|---|
| 0 | 2 | `magic` | `0x5AE7` |
| 2 | 1 | `version` | 2 |
| 3 | 1 | `msg_type` | 1 for events, 2 for transactions |
| 4 | 1 | `flags` | reserved, 0 |
| 5 | 1 | `frag_index` | 0-based fragment index |
| 6 | 1 | `frag_count` | 1 when the frame is not fragmented |
| 7 | 1 | `padding` | 0 |
| 8 | 8 | `seq` | sequence number of the datagram |

For a transaction frame the payload is the slot as a little-endian `uint64`
followed by the raw Solana wire transaction, and `frag_count` is always 1.
`seq` increments once per datagram, whatever the message type.

```go
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

func ParseHeader(buf []byte) (FrameHeader, *FrameError)
```
`ParseHeader` checks length, magic and version, returning `ErrTooShort`,
`ErrBadMagic` or `ErrUnsupportedVersion`.

```go
type FrameError struct{ /* unexported fields */ }

func (e *FrameError) Error() string
func (e *FrameError) Code() string

var ErrTooShort           *FrameError // code "too_short"
var ErrBadMagic           *FrameError // code "bad_magic"
var ErrUnsupportedVersion *FrameError // code "unsupported_version"
var ErrTruncated          *FrameError // code "truncated"
var ErrBadFragment        *FrameError // code "bad_fragment"
```

```go
type PushResult int

const (
	PushMessage PushResult = iota // a transaction was decoded
	PushSkipped                   // another message type; counted and ignored
	PushError                     // rejected frame, see the FrameError
)

type StreamDecoder struct{ /* unexported fields */ }

func NewStreamDecoder() *StreamDecoder
func (d *StreamDecoder) Push(datagram []byte) (update *TransactionUpdate, res PushResult, frameErr *FrameError)
func (d *StreamDecoder) Stats() Stats
```
A `StreamDecoder` is not safe for concurrent use.

```go
decoder := decodedshredstream.NewStreamDecoder()
for {
	n, _, err := conn.ReadFromUDP(buf)
	if err != nil {
		return err
	}
	if u, res, ferr := decoder.Push(buf[:n]); res == decodedshredstream.PushMessage {
		fmt.Println(u.Slot, len(u.Bytes()))
	} else if res == decodedshredstream.PushError {
		fmt.Println("rejected:", ferr.Code())
	}
}
```

### Package `pb`

Generated protobuf and gRPC bindings for the service. `GRPCClient` uses them
internally; import them directly only if you build your own gRPC client.

The gRPC contract is `proto/decoded.proto`, which declares the service and
imports `proto/shreder_binary.proto` for its message types. Both files are
included, and generating bindings in another language needs the two together.

## ⚙️ Performance notes

- **`Run` versus `Updates`.** `Run` calls your handler on the goroutine that
  reads the socket or the stream: no queue, no channel hand-off. `Updates`
  adds one buffered channel for the convenience of `select` loops. Only one
  of the two may be active on a client at a time.
- **Keep the handler short.** Nothing is read while your handler runs. Copy
  what you need, hand it to your own pipeline, return. A consumer that falls
  behind loses data: the server closes a lagging gRPC stream, the `Updates`
  channel discards its oldest update (`QueueDropped`), and a UDP consumer
  loses datagrams once the kernel buffer fills.
- **UDP receive buffer.** The client requests 64 MiB by default
  (`UDPConfig.RecvBufferBytes`) because slot boundaries arrive in bursts.
  Operating systems cap that request: raise `net.core.rmem_max` on Linux or
  `kern.ipc.maxsockbuf` on macOS, and watch `NoticeRecvBufferClamped`.

## 📊 Statistics

`Stats()` returns a snapshot of counters that are cumulative since the
client was created.

| Field | Type | Meaning |
|---|---|---|
| `Events` | `uint64` | Transaction updates delivered to the consumer. |
| `Messages` | `uint64` | Wire messages decoded, one per transaction. |
| `Bytes` | `uint64` | Bytes received: whole datagrams over UDP, transaction bytes over gRPC. |
| `Datagrams` | `uint64` | Datagrams received (UDP). |
| `DecodeErrors` | `uint64` | Datagrams or messages rejected as undecodable. |
| `SkippedMsgType` | `uint64` | Valid frames ignored because they carry another message type. |
| `QueueDropped` | `uint64` | Updates the `Updates()` channel discarded because the consumer was too slow. |
| `Reconnects` | `uint64` | Successful gRPC reconnections. |
| `LastSlot` | `uint64` | Slot of the last transaction delivered. |
| `RecvBufferBytes` | `uint64` | Receive buffer size the operating system granted, in bytes (UDP; 0 over gRPC). |

```go
s := client.Stats()
fmt.Printf("transactions=%d queue_dropped=%d decode_errors=%d last_slot=%d\n",
	s.Events, s.QueueDropped, s.DecodeErrors, s.LastSlot)
```

Comparing `RecvBufferBytes` with the size you requested detects an operating
system clamp without registering a notice callback.
