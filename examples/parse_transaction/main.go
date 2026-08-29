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
			log.Fatal(err)
		}
		port = p
	}

	client, err := decodedshredstream.NewUDP(decodedshredstream.UDPConfig{Port: port})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	err = client.Run(ctx, func(u *decodedshredstream.TransactionUpdate) {
		sig, _ := u.Signature()
		fmt.Printf("slot=%d sig=%s\n", u.Slot, sig)

		msg, err := u.Parse()
		if err != nil {
			fmt.Fprintf(os.Stderr, "  %v\n", err)
			return
		}
		fmt.Printf("  version=%d accounts=%d instructions=%d\n",
			msg.Version, len(msg.StaticAccounts), len(msg.Instructions))
		for _, ix := range msg.Instructions {
			program := msg.StaticAccounts[ix.ProgramIndex]
			fmt.Printf("    %s (%d bytes)\n",
				decodedshredstream.Base58(program), len(ix.Data))
		}
	})
	if err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}
