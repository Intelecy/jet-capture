package jetcapture

import (
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// NewAppSkeleton returns an opinionated, partially filled out cli app struct. The caller is responsible for setting any
// additional app properties (i.e. name), adding cli flags, and completing the options struct (via the setup callback)
// See /apps/simple/main.go for an example
func NewAppSkeleton[P Payload, K DestKey](setup func(c *cli.Context, options *Options[P, K]) error) *cli.App {
	app := cli.NewApp()

	app.Suggest = true

	app.Flags = []cli.Flag{
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
		&cli.StringFlag{
			Name:  "compression",
			Value: string(None),
			Usage: `choose from "none", "gzip", or "snappy"`,
		},
		&cli.BoolFlag{
			Name:  "log-json",
			Usage: "set log format to JSON",
		},
		&cli.StringFlag{
			Name:  "log-level",
			Usage: "set log level",
			Value: "info",
		},
	}

	app.Before = func(c *cli.Context) error {
		if c.Bool("log-json") {
			cfg := zap.NewProductionEncoderConfig()
			cfg.EncodeTime = zapcore.ISO8601TimeEncoder

			// the following settings are based on datadog
			cfg.MessageKey = "message"
			cfg.TimeKey = "timestamp"
			cfg.NameKey = "logger.name"
			cfg.StacktraceKey = "error.stack"

			level, err := zapcore.ParseLevel(c.String("log-level"))
			if err != nil {
				return err
			}

			zlog := zap.New(zapcore.NewCore(zapcore.NewJSONEncoder(cfg), os.Stderr, level))
			SetDefaultLogger(zlog)
		}
		return nil
	}

	app.Action = func(c *cli.Context) error {
		options := DefaultOptions[P, K]()

		if c.IsSet("nats-context") {
			options.NATS.Context = c.String("nats-context")
		} else {
			options.NATS.Server = c.String("nats-server")
			options.NATS.Credentials = c.String("nats-creds")
		}

		options.NATS.StreamName = c.String("stream-name")
		options.NATS.ConsumerName = c.String("consumer-name")
		options.MaxAge = c.Duration("max-age")
		options.BufferToDisk = c.Bool("buffer-to-disk")
		options.Compression = Compression(c.String("compression"))
		options.TempDir = c.Path("tmp-dir")

		if setup != nil {
			if err := setup(c, options); err != nil {
				return err
			}
		}

		return options.Build().Run(c.Context)
	}

	return app
}
