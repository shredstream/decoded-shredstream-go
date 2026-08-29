package main

import (
	"bufio"
	"context"
	"encoding/binary"
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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	out := bufio.NewWriterSize(os.Stdout, 1<<20)
	defer out.Flush()

	var header [12]byte
	err = client.Run(ctx, func(u *decodedshredstream.TransactionUpdate) {
		binary.LittleEndian.PutUint64(header[0:8], u.Slot)
		binary.LittleEndian.PutUint32(header[8:12], uint32(len(u.Bytes())))
		if _, err := out.Write(header[:]); err != nil {
			log.Fatal(err)
		}
		if _, err := out.Write(u.Bytes()); err != nil {
			log.Fatal(err)
		}
	})
	if err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}
