package jetcapture

import (
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/oklog/ulid/v2"
)

var (
	ackAck = []byte("+ACK")
	ackNak = []byte("-NAK")
)

type dataBlock[P Payload] struct {
	id            string
	start         time.Time
	messageCount  int
	rowCount      int
	closed        bool
	writer        FormattedDataWriter[P]
	buffer        buffer
	acks          []string
	newestMessage time.Time
}

func newDataBlock[P Payload](
	start time.Time,
	writer FormattedDataWriter[P],
	buffer buffer,
) *dataBlock[P] {
	b := &dataBlock[P]{
		start:  start,
		writer: writer,
		buffer: buffer,
		acks:   []string{},
	}

	writer.InitNew(buffer)

	b.id = ulid.MustNew(ulid.Timestamp(start), ulid.DefaultEntropy()).String()

	return b
}

func (b *dataBlock[P]) path() string {
	return fmt.Sprintf(
		"%4d/%02d/%02d/%02d/%02d/",
		b.start.Year(), b.start.Month(), b.start.Day(), b.start.Hour(), b.start.Minute(),
	)
}

func (b *dataBlock[P]) fileName(prefix, suffix string) string {
	return fmt.Sprintf(
		"%s-%s.%s",
		prefix,
		b.id,
		suffix,
	)
}

func (b *dataBlock[P]) close() error {
	b.closed = true
	return b.buffer.DoneWriting()
}

func (b *dataBlock[P]) ackAll(nc *nats.Conn) (int, error) {
	acked := 0
	// TODO(jonathan): this acks in the _reverse_ order... does it matter?
	for _, ack := range b.acks {
		if err := nc.Publish(ack, ackAck); err != nil {
			log.Errorf("ack error: %v", err)
		} else {
			acked++
		}
	}
	return acked, nc.Flush()
}

func (b *dataBlock[P]) write(payload P, ack string, md *nats.MsgMetadata) error {
	if md.Timestamp.After(b.newestMessage) {
		b.newestMessage = md.Timestamp
	}
	b.messageCount += 1
	rows, err := b.writer.Write(payload)
	if err != nil {
		// nacks?
	} else {
		b.acks = append(b.acks, ack)
	}
	b.rowCount += rows
	return err
}

func (b *dataBlock[P]) Read(p []byte) (int, error) {
	if !b.closed {
		panic("invalid block state")
	}
	return b.buffer.Read(p)
}
