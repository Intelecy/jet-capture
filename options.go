package jetcapture

import (
	"errors"
	"os"
	"time"

	"github.com/nats-io/nats.go"
)

type Payload interface {
	any
}

type DestKey interface {
	comparable
}

type Compression string

const (
	None   Compression = "none"
	GZip               = "gzip"
	Snappy             = "snappy"
)

const (
	DefaultMaxAge = time.Minute * 15
)

type Options[P Payload, K DestKey] struct {
	NATSStreamName   string        // which stream should jetcapture bind to
	NATSConsumerName string        // which consumer should jetcapture bind to
	Compression      Compression   // apply compression to the resulting files
	Suffix           string        // add a suffix
	BufferToDisk     bool          // should jetcapture buffer to disk using temp files, or keep blocks in memory
	MaxAge           time.Duration // what is the max duration for a single block
	MaxMessages      int           // rough limit to the number of messages in a block before a new one is created
	TempDir          string        // override the default OS temp dir

	// TODO
	// MaxSize        int

	// TODO
	// WriteEmptyFile bool

	MessageDecoder  func(*nats.Msg) (P, K, error)
	WriterFactory   func() FormattedDataWriter[P]
	Store           BlockStore[K]
	OnStoreComplete func(K, string, int64, time.Duration, error) // optional callback for metrics capture
}

func (o *Options[P, K]) Build() *Capture[P, K] {
	return New[P, K](*o)
}

func (o *Options[P, K]) Validate() error {
	if o.Compression == _EMPTY_ {
		o.Compression = None
	}

	switch o.Compression {
	case Snappy, GZip, None:
	default:
		return errors.New("unknown compression type")
	}

	if o.NATSStreamName == _EMPTY_ {
		return errors.New("stream name not set")
	}

	if o.NATSConsumerName == _EMPTY_ {
		return errors.New("consumer name not set")
	}

	if o.MessageDecoder == nil {
		return errors.New("MessageDecoder not set")
	}

	if o.WriterFactory == nil {
		return errors.New("WriterFactory not set")
	}

	if o.Store == nil {
		return errors.New("Store not set")
	}

	if o.MaxAge == 0 {
		o.MaxAge = DefaultMaxAge
	}

	return nil
}

func DefaultOptions[P Payload, K DestKey]() *Options[P, K] {
	options := &Options[P, K]{
		Compression: None,
		MaxAge:      DefaultMaxAge,
		MaxMessages: 0,
		TempDir:     os.TempDir(),
	}

	return options
}
