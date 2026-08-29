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
	endpoint := os.Getenv("DECODED_SHREDSTREAM_ENDPOINT")
	token := os.Getenv("DECODED_SHREDSTREAM_TOKEN")
	if endpoint == "" || token == "" {
		log.Fatal("set DECODED_SHREDSTREAM_ENDPOINT and DECODED_SHREDSTREAM_TOKEN")
	}

	client, err := decodedshredstream.NewGRPC(decodedshredstream.GRPCConfig{
		Endpoint: endpoint,
		Token:    token,
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
		fmt.Printf("slot=%d sig=%s matched=%v\n", u.Slot, sig, u.Filters)
	})
	if err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}
