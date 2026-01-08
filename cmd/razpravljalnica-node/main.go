package main

import (
	"flag"
	"log"
	"os"
	"strings"

	"github.com/FilipGjorgjeski/razpravljalnica/internal/server"
	"github.com/FilipGjorgjeski/razpravljalnica/storage"
)

func main() {
	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	listen := fs.String("listen", "127.0.0.1:0", "gRPC listen address")
	advertise := fs.String("advertise", "", "advertised gRPC address (for other nodes/clients)")
	nodeID := fs.String("node-id", "", "node identifier (must match control-plane --chain order)")
	controlPlane := fs.String("control-plane", "", "control-plane gRPC address")
	_ = fs.Parse(os.Args[1:])

	if strings.TrimSpace(*nodeID) == "" {
		log.Fatalf("--node-id is required")
	}
	if *advertise == "" {
		*advertise = *listen
	}

	st := storage.New()
	h := server.NewHub()
	n := server.NewChainNode(st, h, *nodeID, *listen, *advertise, *controlPlane)

	log.Printf("node %s listening=%s advertise=%s control-plane=%s", *nodeID, *listen, *advertise, *controlPlane)
	if err := n.ListenAndServe(); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
