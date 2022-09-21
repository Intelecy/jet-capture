package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/Intelecy/jet-capture"
	"github.com/nats-io/nats.go"
	"github.com/urfave/cli/v2"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	app := &cli.App{
		Name: "JetStream Capture",
		Authors: []*cli.Author{{
			Name:  "Jonathan Camp",
			Email: "jonathan.camp@intelecy.com",
		}},
		Copyright: "2022 Intelecy AS",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "nats-context",
				EnvVars: []string{"NATS_CONTEXT"},
				Usage:   "NATS context name",
			},
			&cli.StringFlag{
				Name:    "nats-server",
				EnvVars: []string{"NATS_URL"},
				Value:   nats.DefaultURL,
			},
			&cli.PathFlag{
				Name:    "nats-creds",
				EnvVars: []string{"NATS_CREDS"},
				Usage:   "NATS user credentials",
			},
			&cli.StringFlag{
				Name:     "stream-name",
				Aliases:  []string{"s"},
				Required: true,
			},
			&cli.StringFlag{
				Name:     "consumer-name",
				Aliases:  []string{"c"},
				Required: true,
			},
			&cli.DurationFlag{
				Name:  "max-age",
				Value: time.Minute * 15,
			},
			&cli.BoolFlag{
				Name:  "buffer-to-disk",
				Value: true,
				Usage: "buffer data to disk or memory",
			},
			&cli.PathFlag{
				Name:  "tmp-dir",
				Value: os.TempDir(),
				Usage: "temporary directory if buffering data to disk",
			},
			&cli.PathFlag{
				Name:     "output",
				Required: true,
				Usage:    "local output directory",
			},
			&cli.StringFlag{
				Name:  "compression",
				Value: string(jetcapture.None),
				Usage: `choose from "none", "gzip", or "snappy"`,
			},
		},
		Action: func(c *cli.Context) error {
			type (
				payload = *jetcapture.NatsMessage
				destKey = string
			)

			options := jetcapture.DefaultOptions[payload, destKey]()

			if c.IsSet("nats-context") {
				options.NATS.Context = c.String("nats-context")
			} else {
				options.NATS.Server = c.String("nats-server")
				options.NATS.Credentials = c.String("nats-credentials")
			}

			options.NATS.StreamName = c.String("stream-name")
			options.NATS.ConsumerName = c.String("consumer-name")

			options.Suffix = "json"

			options.MessageDecoder = jetcapture.NatsToJson[destKey](jetcapture.SubjectToDestKey)
			options.WriterFactory = func() jetcapture.FormattedDataWriter[payload] {
				return &jetcapture.NewLineDelimitedJSON[payload]{}
			}

			options.MaxAge = c.Duration("max-age")
			options.BufferToDisk = c.Bool("buffer-to-disk")
			options.Compression = jetcapture.Compression(c.String("compression"))

			options.Store = jetcapture.SingleDirStore[destKey](c.Path("output"))

			return options.Build().Run(c.Context)
		},
	}

	if err := app.RunContext(ctx, os.Args); err != nil {
		log.Fatalln(err)
	}
}
