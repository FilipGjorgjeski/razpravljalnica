package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/FilipGjorgjeski/razpravljalnica/client"
	"github.com/FilipGjorgjeski/razpravljalnica/gui"
)

func main() {
	err := run()
	if err != nil {
		slog.Error("Running failed", "err", err)
		os.Exit(1)
	}

}

func run() error {
	var cpAddr string
	flag.StringVar(&cpAddr, "control-plane", "127.0.0.1:50050", "control-plane gRPC address")
	flag.Parse()

	c, err := client.New(cpAddr)
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }()

	ctx, cancel := client.WithTimeout(context.Background(), 5*time.Second)
	_, err = c.RefreshClusterState(ctx)
	cancel()
	if err != nil {
		return err
	}

	watchCtx, watchCancel := context.WithCancel(context.Background())
	defer watchCancel()
	go func() {
		if err := c.WatchClusterState(watchCtx); err != nil {
			fmt.Fprintf(os.Stderr, "watch stopped: %v\n", err)
		}
	}()

	app := gui.NewApp(c)

	return app.Run()
}
