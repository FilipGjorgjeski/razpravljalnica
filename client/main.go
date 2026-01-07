package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/FilipGjorgjeski/razpravljalnica/client/connection"
	"github.com/FilipGjorgjeski/razpravljalnica/client/ui"
	"github.com/urfave/cli/v3"
)

func main() {
	err := run()
	if err != nil {
		slog.Error("Running failed", "err", err)
		os.Exit(1)
	}

}

func run() error {
	cmd := &cli.Command{
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "url",
				Value: "localhost",
				Usage: "url to connect to",
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			conn := connection.NewConnection(cmd.String("url"))
			defer conn.Disconnect()

			app := ui.NewUI(conn)

			return app.Run()
		},
	}

	return cmd.Run(context.Background(), os.Args)

}
