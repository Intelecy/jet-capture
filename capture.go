package jetcapture

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/snappy"
	"github.com/nats-io/jsm.go/natscontext"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	ErrUnknownCompression = errors.New("unknown compression type")
)

const (
	_EMPTY_ = ""
)

var log *zap.SugaredLogger

func init() {
	cfg := zap.NewDevelopmentConfig()
	cfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	_log, _ := cfg.Build()
	log = _log.Sugar()
}

type message[P Payload, K DestKey] struct {
	msg     *nats.Msg
	Payload P
	DestKey K
}

func (m *message[P, K]) RawMessage() *nats.Msg {
	return m.msg
}

type Capture[P Payload, K DestKey] struct {
	opts Options[P, K]
	nc   *nats.Conn
	js   nats.JetStreamContext

	fetched int
	acked   int

	blocks map[K][]*dataBlock[P]

	newestMessage time.Time

	start time.Time
}

func NewCapture[P Payload, K DestKey](opts Options[P, K]) *Capture[P, K] {
	return &Capture[P, K]{
		opts:   opts,
		blocks: map[K][]*dataBlock[P]{},
	}
}

func (c *Capture[P, K]) Run(ctx context.Context) error {
	switch c.opts.Compression {
	case Snappy:
	case GZip:
	case None:
	default:
		return ErrUnknownCompression
	}

	var (
		wg  sync.WaitGroup
		err error
	)

	options := []nats.Option{
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			log.Error(err)
		}),
		nats.ClosedHandler(func(*nats.Conn) {
			log.Info("nats.ClosedHandler")
			wg.Done()
		}),
	}

	if c.opts.NATS.Context != "" {
		nctx, err := natscontext.New(
			c.opts.NATS.Context,
			true,
		)
		if err != nil {
			return err
		}

		if c.nc, err = nctx.Connect(options...); err != nil {
			return err
		}
	} else {
		options = append(options, nats.UserCredentials(c.opts.NATS.Credentials))
		if c.nc, err = nats.Connect(c.opts.NATS.Server, options...); err != nil {
			return err
		}
	}

	wg.Add(1)

	if c.js, err = c.nc.JetStream(); err != nil {
		return err
	}

	cinfo, _ := c.js.ConsumerInfo(c.opts.NATS.StreamName, c.opts.NATS.ConsumerName)
	_ = cinfo

	// TODO(jonathan): check acktimeout and compare to opts.MaxAge

	sub, err := c.js.PullSubscribe(_EMPTY_, c.opts.NATS.ConsumerName, nats.Bind(c.opts.NATS.StreamName, c.opts.NATS.ConsumerName))
	if err != nil {
		return err
	}

	defer func() {
		c.sweepBlocks(ctx, true)

		log.Infof("fetched: %d, acked: %d", c.fetched, c.acked)

		log.Infof("Drain sub...")
		if err := sub.Drain(); err != nil {
			log.Errorf("sub.Unsubscribe err: %v", err)
		}

		log.Infof("draining nc...")
		if err := c.nc.Drain(); err != nil {
			log.Errorf("nc.Drain err: %v", err)
		}

		wg.Wait()

		log.Infof("exiting")
		log.Infof("nc.IsClosed: %v", c.nc.IsClosed())
	}()

	c.start = time.Now()

	for {
		if err := c.fetch(ctx, sub, cinfo.Config.MaxRequestBatch); err != nil {
			if err == context.Canceled {
				err = nil
			}
			return err
		}

		c.sweepBlocks(ctx, false)
	}
}

func (c *Capture[P, K]) sweepBlocks(ctx context.Context, final bool) {
	for dk, v := range c.blocks {
		var keep []*dataBlock[P]

		for _, b := range v {
			if final || c.newestMessage.After(b.start.Add(c.opts.MaxAge)) || (c.opts.MaxMessages > 0 && b.messageCount >= c.opts.MaxMessages) {
				if err := c.finalizeBlock(ctx, b, dk); err != nil {
					log.Error(err)
				}
			} else {
				keep = append(keep, b)
			}
		}

		c.blocks[dk] = keep
	}

	elapsed := time.Since(c.start)

	log.Infof("messages per second: %d", int(float64(c.acked)/elapsed.Seconds()))
}

func (c *Capture[P, K]) finalizeBlock(ctx context.Context, block *dataBlock[P], dk K) error {
	defer func() {
		block.buffer.Remove()
	}()

	if err := block.close(); err != nil {
		return err
	}

	if _, err := c.opts.Store.Write(ctx, block, dk, block.path(), block.fileName("backup", c.fileSuffix())); err != nil {
		return err
	}

	acked, err := block.ackAll(c.nc)
	if err != nil {
		return err
	}

	c.acked += acked

	return nil
}

func (c *Capture[P, K]) fileSuffix() string {
	suffix := c.opts.Suffix

	switch c.opts.Compression {
	case Snappy:
		suffix += ".snappy"
	case GZip:
		suffix += ".gz"
	}

	return suffix
}

func (c *Capture[P, K]) fetch(ctx context.Context, sub *nats.Subscription, batchSz int) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	messages, err := sub.Fetch(batchSz, nats.Context(ctx))
	c.fetched += len(messages)

	if err != nil {
		return err
	}

	if len(messages) != batchSz {
		log.Infof("len(messages) != batchSz: %d %d", len(messages), batchSz)
	}

	for _, m := range messages {
		md, _ := m.Metadata()

		if md.Timestamp.After(c.newestMessage) {
			c.newestMessage = md.Timestamp
		}

		decoded, dk, err := c.opts.MessageDecoder(m)
		if err != nil {
			log.Error(err)
			continue
		}

		msg := &message[P, K]{
			msg:     m,
			Payload: decoded,
			DestKey: dk,
		}

		block, err := c.findBlock(msg, md)
		if err != nil {
			log.Error(err)
			continue

		}

		if err := block.write(msg.Payload, m.Reply, md); err != nil {
			log.Error(err)
			continue
		}
	}

	return nil
}

func (c *Capture[P, K]) findBlock(msg *message[P, K], md *nats.MsgMetadata) (*dataBlock[P], error) {
	dk := msg.DestKey

	start := md.Timestamp.Truncate(c.opts.MaxAge)

	var block *dataBlock[P]

	if _, ok := c.blocks[dk]; !ok {
		c.blocks[dk] = []*dataBlock[P]{}
	} else {
		for _, b := range c.blocks[dk] {
			if b.start.Equal(start) {
				block = b
				break
			}
		}
	}

	if block == nil {
		log.Info("creating a new block...")
		buf, err := c.makeBuffer()
		if err != nil {
			return nil, err
		}
		block = newDataBlock[P](start, c.opts.WriterFactory(), buf)
		c.blocks[dk] = append(c.blocks[dk], block)
	}

	return block, nil
}

func (c *Capture[P, K]) makeBuffer() (buffer, error) {
	var (
		buf buffer
		err error
	)

	if c.opts.BufferToDisk {
		if buf, err = newDiskBuffer(c.opts.TempDir); err != nil {
			return nil, err
		}
	} else {
		buf = newMemoryBuffer()
	}

	switch c.opts.Compression {
	case Snappy:
		buf = &wrappedWriter{
			buffer: buf,
			wr:     snappy.NewBufferedWriter(buf),
		}
	case GZip:
		buf = &wrappedWriter{
			buffer: buf,
			wr:     gzip.NewWriter(buf),
		}
	case None:
	default:
		panic("unhandled compression type")
	}

	return buf, nil
}
