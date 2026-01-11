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

	var raftCfg controlplane.RaftConfig
	raftCfg.RegisterFlags(fs)

	_ = fs.Parse(os.Args[1:])

	srv := controlplane.NewWithRAFT(cfg, raftCfg)

	if err := srv.InitRAFT(); err != nil {
		log.Fatalf("failed to initialize RAFT: %s", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		os.Exit(0)
	}()

	if raftCfg.Enabled {
		log.Printf("control-plane starting with RAFT enabled")
		log.Printf("gRPC listen: %s", *listen)
		log.Printf("RAFT node-id: %s", raftCfg.NodeID)
		log.Printf("RAFT bind:  %s", raftCfg.BindAddr)
		log.Printf("chain: %v", cfg.ChainOrder)
	} else {
		log.Printf("control-plane starting with RAFT disabled")
		log.Printf("gRPC listen: %s", *listen)
		log.Printf("chain: %v", cfg.ChainOrder)
	}

	// Start gRPC server (blocks until error or shutdown)
	if err := controlplane.ListenAndServe(*listen, srv); err != nil {
		log.Fatalf("serve error: %v", err)
	}
}
