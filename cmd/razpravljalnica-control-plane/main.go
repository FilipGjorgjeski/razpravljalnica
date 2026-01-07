package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/FilipGjorgjeski/razpravljalnica/internal/controlplane"
)

func main() {
	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	listen := fs.String("listen", "127.0.0.1:50050", "gRPC listen address")

	var cfg controlplane.Config
	cfg.HeartbeatTimeout = 3 * time.Second
	cfg.RegisterFlags(fs)
	_ = fs.Parse(os.Args[1:])

	srv := controlplane.New(cfg)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		os.Exit(0)
	}()

	log.Printf("control-plane listening on %s (chain=%v)", *listen, cfg.ChainOrder)
	if err := controlplane.ListenAndServe(*listen, srv); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
