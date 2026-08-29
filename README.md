# Decoded ShredStream — Go client

Go client for the Decoded ShredStream of ShredStream.com: pre-execution
Solana transactions, decoded from shreds — the serialized
`VersionedTransaction`, its signatures and its slot, delivered over gRPC or
UDP push the moment they propagate.

> **Before execution** — transactions carry no status, logs, balance changes
> or inner instructions, and some will fail on-chain. Use a post-execution
> source to confirm.

```sh
go get github.com/shredstream/decoded-shredstream-go
```

```go
import decodedshredstream "github.com/shredstream/decoded-shredstream-go"

client, err := decodedshredstream.NewGRPC(decodedshredstream.GRPCConfig{
	Endpoint: endpoint,
	Token:    token,
	Filters:  decodedshredstream.Filters{"all": decodedshredstream.FilterAll()},
})
if err != nil {
	log.Fatal(err)
}
defer client.Close()

err = client.Run(ctx, func(u *decodedshredstream.TransactionUpdate) {
	sig, _ := u.Signature()
	fmt.Println(u.Slot, sig)
})
```

> **Requirements** — Go 1.25 or later, and a Decoded ShredStream subscription
> on ShredStream.com.

## 🔑 Access

Decoded ShredStream is a subscription product, available from ShredStream.com.
One subscription covers both transports, and you can move from one to the
other whenever you need to.

- **gRPC** — you receive an endpoint and an access token. Use the endpoint
  exactly as issued.
- **UDP** — you register your server's IP and port; datagrams are pushed to it.

### Choosing a transport

Both carry the same data; they differ on what the protocol guarantees.

| | gRPC | UDP |
|---|---|---|
| Latency | higher | **lowest** |
| Delivery | ordered, retransmitted | best-effort, no retransmission |
| Server-side filters | **yes** | no — you receive the full stream |

## ⚡ Quickstart — gRPC

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"

	decodedshredstream "github.com/shredstream/decoded-shredstream-go"
)

func main() {
	client, err := decodedshredstream.NewGRPC(decodedshredstream.GRPCConfig{
		Endpoint: os.Getenv("DECODED_SHREDSTREAM_ENDPOINT"),
		Token:    os.Getenv("DECODED_SHREDSTREAM_TOKEN"),
		Filters:  decodedshredstream.Filters{"all": decodedshredstream.FilterAll()},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	err = client.Run(ctx, func(u *decodedshredstream.TransactionUpdate) {
		sig, _ := u.Signature()
		fmt.Printf("slot=%d sig=%s matched=%v %d bytes\n",
			u.Slot, sig, u.Filters, len(u.Bytes()))
	})
	if err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}
```

`NewGRPC` connects and subscribes before returning.

## 📡 Quickstart — UDP

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"

	decodedshredstream "github.com/shredstream/decoded-shredstream-go"
)

func main() {
	client, err := decodedshredstream.NewUDP(decodedshredstream.UDPConfig{Port: 8002})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	fmt.Println("listening on", client.LocalAddr())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	err = client.Run(ctx, func(u *decodedshredstream.TransactionUpdate) {
		sig, _ := u.Signature()
		fmt.Printf("slot=%d sig=%s %d bytes\n", u.Slot, sig, len(u.Bytes()))
	})
	if err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}
```

`8002` is only an example: bind whichever port you registered in your
account.

## 🔍 Transaction parsing

Every transaction exposes `Bytes()`, in the standard Solana wire format, and
its signatures without any decoding:

```go
sig, ok := u.Signature()  // fee payer signature
all := u.Signatures()     // every signature, in wire order
```

Everything else is available through `Parse`:

```go
err = client.Run(ctx, func(u *decodedshredstream.TransactionUpdate) {
	msg, err := u.Parse()
	if err != nil {
		log.Printf("slot=%d: %v", u.Slot, err)
		return
	}
	fmt.Printf("slot=%d version=%d accounts=%d instructions=%d\n",
		u.Slot, msg.Version, len(msg.StaticAccounts), len(msg.Instructions))

	for _, ix := range msg.Instructions {
		program := msg.StaticAccounts[ix.ProgramIndex]
		fmt.Printf("  %s (%d bytes)\n", decodedshredstream.Base58(program), len(ix.Data))
	}
})
```

## 🎯 Filters

Filters apply to gRPC only and are evaluated server-side; the client never
drops a transaction locally. A subscription declares a map of named filters,
and every update lists the names it matched in `u.Filters`.

A `Filter` holds three lists of base58 account keys, matched against the
accounts the transaction touches:

| List | A transaction matches when it touches… |
|---|---|
| `Include` | at least one of these accounts |
| `Exclude` | none of these accounts |
| `Required` | all of these accounts |

The three conditions are combined with a logical AND, and a list left empty
adds no constraint. A filter with all three lists empty therefore matches
every transaction; `FilterAll()` returns exactly that, and a transaction
matching several named filters is delivered once.

Matching uses the account keys carried in the transaction, signers included.
Addresses resolved through a lookup table cannot be filtered on.

```go
filters := decodedshredstream.Filters{
	"watched-wallet": {Include: []string{wallet}},
	"pair":           {Required: []string{mintA, mintB}},
	"everything":     decodedshredstream.FilterAll(),
}
```

The map can be replaced on a live stream, without reconnecting and without
interrupting delivery. The server swaps it atomically, and the new map is
also what any later reconnection re-sends:

```go
err := client.UpdateFilters(decodedshredstream.Filters{
	"watched-wallet": {Include: []string{wallet}},
})
```

## 🔄 Errors & reconnection

Recoverable interruptions never reach you: the client reconnects on its own and
re-sends the current filter map. Only a refused token, a session closed by the
server and a rejected filter map end the stream — `Run` returns the error, and
after the `Updates()` channel closes `Err()` reports it:

```go
if err := client.Run(ctx, handler); err != nil && ctx.Err() == nil {
	log.Fatal(err)
}
```

Every error value, the backoff policy and the telemetry notices are in
[docs/errors.md](docs/errors.md).

## 📖 Documentation

This README is what you need to receive transactions. The rest lives beside it:

| Document | Contents |
|---|---|
| [docs/api.md](docs/api.md) | Every type and method: clients, configuration, filters, updates, UDP codec, performance notes and counters |
| [docs/errors.md](docs/errors.md) | Error types, reconnection policy, telemetry notices |

## 💡 Examples

Runnable programs live in `examples/`:

| Example | Shows |
|---|---|
| `udp_quickstart` | Binding a port and printing incoming transactions. |
| `grpc_quickstart` | Connecting, subscribing to everything, printing matches. |
| `grpc_filters` | Named filters and replacing the map on a live stream. |
| `parse_transaction` | Cheap accessors first, then `Parse` for the message. |
| `raw_bytes_pipeline` | Forwarding wire bytes without decoding them. |
| `low_latency` | The inline-callback consumption pattern. |

```sh
DECODED_SHREDSTREAM_UDP_PORT=8002 go run ./examples/udp_quickstart
DECODED_SHREDSTREAM_ENDPOINT=your-endpoint.shredstream.com:PORT DECODED_SHREDSTREAM_TOKEN=... go run ./examples/grpc_quickstart
DECODED_SHREDSTREAM_ENDPOINT=your-endpoint.shredstream.com:PORT DECODED_SHREDSTREAM_TOKEN=... WALLET=<base58> go run ./examples/grpc_filters
```

## ⚖️ License

Apache-2.0. See `LICENSE`.
