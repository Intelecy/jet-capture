package jetcapture

import (
	"time"

	"github.com/nats-io/nats.go"
)

type Compression string

const (
	None   Compression = "none"
	GZip               = "gzip"
	Snappy             = "snappy"
)

type DestKey interface {
	comparable
}

type Payload interface {
	any
}

type Options[P Payload, K DestKey] struct {
	NATS struct {
		Context      string
		Server       string
		Credentials  string
		InboxPrefix  string
		StreamName   string
		ConsumerName string
	}

	Compression  Compression
	Suffix       string
	BufferToDisk bool
	MaxAge       time.Duration
	MaxMessages  int
	TempDir      string

	// TODO
	// MaxSize        int

	// TODO
	// WriteEmptyFile bool

	MessageDecoder func(*nats.Msg) (P, K, error)
	WriterFactory  func() FormattedDataWriter[P]
	Store          BlockStore[K]
}

func (o *Options[P, K]) Build() *Capture[P, K] {
	return NewCapture[P, K](*o)
}

func DefaultOptions[P Payload, K DestKey]() *Options[P, K] {
	options := &Options[P, K]{
		Compression: None,
		MaxAge:      time.Minute * 15,
		MaxMessages: 0,
		TempDir:     "",
	}

	options.NATS.Server = nats.DefaultURL

	return options
}
