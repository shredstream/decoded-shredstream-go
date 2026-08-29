package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
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
	fmt.Println("listening on", client.LocalAddr())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	err = client.Run(ctx, func(u *decodedshredstream.TransactionUpdate) {
		sig, _ := u.Signature()
		fmt.Printf("slot=%d sig=%s %d bytes (UDP delivers the full stream)\n",
			u.Slot, sig, len(u.Bytes()))
	})
	if err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}
