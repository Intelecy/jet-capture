package jetcapture

import (
	"github.com/nats-io/nats.go"
	"golang.org/x/exp/maps"
)

type NatsMessage struct {
	Subject  string              `json:"subject"`
	Reply    string              `json:"reply"` // TODO(jonathan): should reply be included?
	Header   map[string][]string `json:"header"`
	Data     []byte              `json:"data"`
	Metadata *nats.MsgMetadata   `json:"metadata"`
}

func NatsToNats[K DestKey](resolve func(msg *nats.Msg) K) func(msg *nats.Msg) (*NatsMessage, K, error) {
	return func(msg *nats.Msg) (*NatsMessage, K, error) {
		dk := resolve(msg)

		nm := &NatsMessage{
			Subject: msg.Subject,
			Reply:   msg.Reply,
			Header:  map[string][]string{},
			Data:    msg.Data,
		}

		var err error

		if nm.Metadata, err = msg.Metadata(); err != nil {
			return nil, dk, err
		}

		maps.Copy(nm.Header, msg.Header)

		return nm, dk, nil
	}
}

func SubjectToDestKey(msg *nats.Msg) string { return msg.Subject }
