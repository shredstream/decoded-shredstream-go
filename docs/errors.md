# Errors and reconnection — Decoded ShredStream Go client

What ends the stream, what the client recovers from on its own, and how it
reports both. See the [README](../README.md) for the common path.

**Terminal errors** end the stream. `Run` returns one of them, and after the
`Updates()` channel closes `Err()` reports it. The client never retries them.
Match with `errors.Is`:

| Error | Cause |
|---|---|
| `ErrAuthRefused` | The server refused the credentials. |
| `ErrKicked` | An operator terminated the session. |
| `ErrInvalidFilter` | The filter map was rejected locally or by the server. The returned error carries the detail. |
| `ErrClosed` | `Close` was called. |
| `ctx.Err()` | The context passed to `Run` was cancelled. |

`Run` also returns an immediate error, matching none of the above, when the
handler is `nil` or when `Run` or `Updates` is already active on the same
client. In UDP mode a failing socket surfaces as the wrapped I/O error.

**Recoverable conditions** never reach the caller in gRPC mode. Transport
breaks, an unexpected end of stream and `DATA_LOSS` (the client consumed too
slowly) cause the client to reconnect and re-send the current filter map on
its own. Delays grow exponentially, with jitter, from `Initial` up to `Max`,
and start over once a connection has been up for `ResetAfter`:

```go
Reconnect: decodedshredstream.ReconnectPolicy{
	Initial:    100 * time.Millisecond,
	Max:        5 * time.Second,
	Multiplier: 2,
	ResetAfter: 30 * time.Second,
},
```

Those are the defaults; any field left at zero takes its own.

**Nothing is replayed.** What the stream emitted while you were disconnected
is gone.

**Notices.** Out-of-band conditions are reported as they happen through the
callback registered with `OnNotice`, rather than by polling the counters:

| `NoticeKind` | Fields | Reports |
|---|---|---|
| `NoticeDecodeError` | — | A frame or message could not be decoded. |
| `NoticeRecvBufferClamped` | `Requested`, `Effective` | The operating system granted a smaller receive buffer than requested. |
| `NoticeReconnecting` | `Attempt`, `Delay` | A reconnection attempt is scheduled. |
| `NoticeReconnected` | — | The stream is connected again. |

```go
client.OnNotice(func(n decodedshredstream.Notice) {
	switch n.Kind {
	case decodedshredstream.NoticeRecvBufferClamped:
		fmt.Fprintf(os.Stderr, "[warn] receive buffer clamped to %d (asked %d)\n",
			n.Effective, n.Requested)
	}
})
```

`NoticeKind.String()` gives a stable lower-case name (`decode_error`,
`recv_buffer_clamped`, `reconnecting`, `reconnected`).
Notices fire on the receive path, so the callback must return quickly.
