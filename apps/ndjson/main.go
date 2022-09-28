package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/Intelecy/jet-capture"
	"github.com/urfave/cli/v2"
)

func main() {
	// since this process is long-running, set up a ctrl-c handler to gracefully shutdown
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// declare some type aliases to make the capture initialization a bit more compact
	type (
		P = *jetcapture.NatsMessage // decoded payload type
		K = string                  // destination key type
	)

	// we use `NewAppSkeleton` to set up some common cli flags. but we also want to
	// add a few more for this specific app
	outputFlag := &cli.PathFlag{
		Name:     "output",
		Required: true,
		Usage:    "local output directory",
	}

	groupByFlag := &cli.BoolFlag{
		Name:  "group-by-subject",
		Value: false,
		Usage: "should the output files be grouped by message subject",
	}

	// initialize the cli app and implement the required callback to finish setting
	// up the capture options struct with our specific decoder, writer, and store
	// and any other overrides.
	app := jetcapture.NewAppSkeleton[P, K](func(c *cli.Context, options *jetcapture.Options[P, K]) error {
		options.Suffix = "json"

		// set up a decoder that just copies the underlying NATS message to a struct that can easily be serialized
		// and also set the destination key to the nats subject
		options.MessageDecoder = jetcapture.NatsToNats[K](jetcapture.SubjectToDestKey)

		// set up a new-line delimited JSON writer
		options.WriterFactory = func() jetcapture.FormattedDataWriter[P] {
			return &jetcapture.NewLineDelimitedJSON[P]{}
		}

		if c.Bool(groupByFlag.Name) {
			options.Store = &jetcapture.LocalFSStore[K]{
				Resolver: func(subject K) (string, error) {
					return filepath.Join(c.Path(outputFlag.Name), subject), nil
				},
			}
		} else {
			// set up a simple local directory store
			options.Store = jetcapture.SingleDirStore[K](c.Path(outputFlag.Name))
		}

		return nil
	})

	// finish filling in app metadata and documentation
	app.Description = "Captures a stream and writes raw NATS messages to a local directory using new-line delimited JSON."
	app.Authors = []*cli.Author{{
		Name:  "Jonathan Camp",
		Email: "jonathan.camp@intelecy.com",
	}}
	app.Copyright = "2022 Intelecy AS"

	// append our additional flags
	app.Flags = append(app.Flags, outputFlag, groupByFlag)

	// and liftoff!
	if err := app.RunContext(ctx, os.Args); err != nil {
		cancel()
		log.Fatal(err)
	}
}
