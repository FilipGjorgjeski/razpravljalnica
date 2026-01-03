package main

import (
	"log/slog"
	"os"

	"github.com/FilipGjorgjeski/razpravljalnica/client/ui"
)

func main() {
	err := run()
	if err != nil {
		slog.Error("Running failed", "err", err)
		os.Exit(1)
	}

}

func run() error {

	app := ui.NewUI()

	return app.Run()
}
