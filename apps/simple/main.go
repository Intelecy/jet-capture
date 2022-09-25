package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Intelecy/jet-capture"
	"github.com/urfave/cli/v2"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	type (
		P = *jetcapture.NatsMessage
		K = string
	)

	outputFlag := &cli.PathFlag{
		Name:     "output",
		Required: true,
		Usage:    "local output directory",
	}

	app := jetcapture.NewAppSkeleton[P, K](func(c *cli.Context, options *jetcapture.Options[P, K]) error {
		options.Suffix = "json"

		options.MessageDecoder = jetcapture.NatsToJson[K](jetcapture.SubjectToDestKey)
		options.WriterFactory = func() jetcapture.FormattedDataWriter[P] {
			return &jetcapture.NewLineDelimitedJSON[P]{}
		}
		options.Store = jetcapture.SingleDirStore[K](c.Path(outputFlag.Name))

		return nil
	})

	app.Name = "JetStream Capture"
	app.Description = "TODO"
	app.Authors = []*cli.Author{{
		Name:  "Jonathan Camp",
		Email: "jonathan.camp@intelecy.com",
	}}
	app.Copyright = "2022 Intelecy AS"
	app.Flags = append(app.Flags, outputFlag)

	if err := app.RunContext(ctx, os.Args); err != nil {
		log.Fatalln(err)
	}
}
