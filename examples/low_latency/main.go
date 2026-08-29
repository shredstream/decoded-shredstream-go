package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"

	decodedshredstream "github.com/shredstream/decoded-shredstream-go"
)

func main() {
	port := 8002
	if v := os.Getenv("DECODED_SHREDSTREAM_UDP_PORT"); v != "" {
		p, err := strconv.Atoi(v)
		if err != nil {
			log.Fatalf("DECODED_SHREDSTREAM_UDP_PORT must be a port number: %v", err)
		}
		port = p
	}
	client, err := decodedshredstream.NewUDP(decodedshredstream.UDPConfig{Port: port})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var count uint64
	err = client.Run(ctx, func(u *decodedshredstream.TransactionUpdate) {
		_ = u.Slot
		_ = u.Bytes() // zero-copy view

		count++
		if count >= 1_000_000 {
			cancel() // stop after 1M transactions (demo)
		}
	})
	if err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
	fmt.Println("done:", count, "transactions")

}
