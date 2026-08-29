package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	decodedshredstream "github.com/shredstream/decoded-shredstream-go"
)

func main() {
	endpoint := os.Getenv("DECODED_SHREDSTREAM_ENDPOINT")
	token := os.Getenv("DECODED_SHREDSTREAM_TOKEN")
	wallet := os.Getenv("WALLET")
	if endpoint == "" || token == "" || wallet == "" {
		log.Fatal("set DECODED_SHREDSTREAM_ENDPOINT, DECODED_SHREDSTREAM_TOKEN and WALLET (base58 account to watch)")
	}

	client, err := decodedshredstream.NewGRPC(decodedshredstream.GRPCConfig{
		Endpoint: endpoint,
		Token:    token,
		Filters: decodedshredstream.Filters{
			"watched-wallet": {Include: []string{wallet}},
			"everything":     decodedshredstream.FilterAll(),
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	started := time.Now()
	var swapOnce sync.Once

	err = client.Run(ctx, func(u *decodedshredstream.TransactionUpdate) {
		fmt.Printf("slot=%d matched=%v\n", u.Slot, u.Filters)

		if time.Since(started) > 10*time.Second {
			swapOnce.Do(func() {
				narrow := decodedshredstream.Filters{"watched-wallet": {Include: []string{wallet}}}
				if err := client.UpdateFilters(narrow); err != nil {
					log.Println("filter swap failed:", err)
					return
				}
				fmt.Println("--- filter map narrowed, stream continues without a gap ---")
			})
		}
	})
	if err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}
