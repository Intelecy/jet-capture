package jetcapture

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type nlog struct {
	*zap.SugaredLogger
}

func (n *nlog) Noticef(format string, v ...interface{}) {
	n.SugaredLogger.Debugf(format, v...)
}

func (n *nlog) Tracef(format string, v ...interface{}) {
	n.SugaredLogger.Debugf(format, v...)
}

// DefaultTestOptions are default options for the unit tests.
var DefaultTestOptions = server.Options{
	Host:                  "127.0.0.1",
	Port:                  4222,
	NoLog:                 false,
	Debug:                 true,
	Trace:                 true,
	NoSigs:                true,
	MaxControlLine:        4096,
	DisableShortFirstPing: true,
}

func runBasicJetStreamServer(t *testing.T) *server.Server {
	opts := DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	return runServer(&opts)
}

func clientConnectToServer(t *testing.T, s *server.Server) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(s.ClientURL(),
		nats.Name("JS-TEST"),
		nats.ReconnectWait(5*time.Millisecond),
		nats.MaxReconnects(-1))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	return nc
}

// New Go Routine based server
func runServer(opts *server.Options) *server.Server {
	s, err := server.NewServer(opts)
	if err != nil || s == nil {
		panic(fmt.Sprintf("No NATS Server object returned: %v", err))
	}

	if !opts.NoLog {
		s.SetLoggerV2(&nlog{log}, false, false, false)
	}

	// Run server in Go routine.
	go s.Start()

	// Wait for accept loop(s) to be started
	if ready := s.ReadyForConnections(10 * time.Second); !ready {
		panic(err)
	}

	return s
}

type testDecodedOrder struct {
	CustomerName string    `json:"customer_name"`
	TimeStamp    time.Time `json:"time_stamp"`
	OrderID      int       `json:"order_id"`
	Contents     string    `json:"contents"`
}

type testOrderDestKey struct {
	CustomerName string
}

type captureTestConfig struct {
	messages        int
	maxAckPending   int
	maxRequestBatch int
	ackWait         time.Duration
	startingOrderID int
}

var things = map[int]string{
	0: "hats",
	1: "shoes",
	2: "pants",
	3: "star destroyer",
}

func validThing(t string) bool {
	for _, v := range things {
		if t == v {
			return true
		}
	}
	return false
}

const (
	streamName   = "STREAM1"
	consumerName = "testConsumer"
)

func initJetStream(t *testing.T, cfg captureTestConfig) (*nats.Conn, nats.JetStreamContext, *server.Server) {
	assert := require.New(t)

	s := runBasicJetStreamServer(t)
	t.Cleanup(s.Shutdown)

	assert.True(s.JetStreamEnabled())

	nc := clientConnectToServer(t, s)
	t.Cleanup(nc.Close)

	assert.Equal(s.ClientURL(), nc.Servers()[0])

	assert.Nil(nc.Publish("foo", nil))
	assert.Nil(nc.Flush())

	js, err := nc.JetStream()
	assert.Nil(err)
	assert.NotNil(js)

	_, err = js.AddStream(&nats.StreamConfig{
		Name:      streamName,
		Subjects:  []string{"orders.>"},
		Retention: nats.LimitsPolicy,
		Storage:   nats.MemoryStorage,
		MaxMsgs:   int64(cfg.messages),
	})
	assert.Nil(err)

	_, err = js.AddConsumer(streamName, &nats.ConsumerConfig{
		Name:            consumerName,
		AckPolicy:       nats.AckExplicitPolicy,
		Durable:         consumerName,
		DeliverPolicy:   nats.DeliverAllPolicy,
		MaxRequestBatch: cfg.maxRequestBatch,
		AckWait:         cfg.ackWait,
		MaxAckPending:   cfg.maxAckPending,
	})
	assert.Nil(err)

	timestamp := time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Second)

	orderID := cfg.startingOrderID

	for i := 0; i < cfg.messages; i++ {
		customerName := string(byte('a' + (i % 26)))
		subj := fmt.Sprintf("orders.%s.%d", customerName, orderID)
		payload := fmt.Sprintf(
			`{"customer_name": "%s", "time_stamp": "%s", "order_id": %d, "contents": "%s"}`,
			customerName,
			timestamp.Format(time.RFC3339),
			orderID,
			things[orderID%len(things)],
		)

		assert.Nil(nc.Publish(subj, []byte(payload)))

		orderID++
		timestamp = timestamp.Add(time.Second)
	}

	assert.Nil(nc.Flush())

	sinfo, err := js.StreamInfo(streamName)
	assert.Nil(err)

	log.Infof("stream ready at: %s", s.ClientURL())

	assert.EqualValues(cfg.messages, sinfo.State.Msgs)

	return nc, js, s
}

func TestCapture(t *testing.T) {
	var (
		err    error
		assert = require.New(t)
	)

	cfg := captureTestConfig{
		messages:        10000,
		maxAckPending:   20000,
		maxRequestBatch: 100,
		ackWait:         time.Minute,
		startingOrderID: 200000,
	}

	_, _, s := initJetStream(t, cfg)

	options := DefaultOptions[*testDecodedOrder, testOrderDestKey]()
	options.NATS.Server = s.ClientURL()
	options.NATS.StreamName = streamName
	options.NATS.ConsumerName = consumerName
	options.MaxAge = 10 * time.Second
	options.MaxMessages = 0
	options.Suffix = "csv"

	options.MessageDecoder = func(m *nats.Msg) (*testDecodedOrder, testOrderDestKey, error) {
		var (
			decoded testDecodedOrder
			key     testOrderDestKey
		)

		if strings.Split(m.Subject, ".")[2] == "200006" {
			panic(errors.New("no bueno!"))
		}

		if err := json.Unmarshal(m.Data, &decoded); err != nil {
			return nil, key, err
		}

		key.CustomerName = decoded.CustomerName

		return &decoded, key, nil
	}

	csvHeader := []string{"customer_name", "time_stamp", "order_id", "contents"}

	options.WriterFactory = func() FormattedDataWriter[*testDecodedOrder] {
		return NewCSVWriter(
			csvHeader,
			func(payload *testDecodedOrder) ([][]string, error) {
				return [][]string{{
					payload.CustomerName,
					payload.TimeStamp.Format(time.RFC3339),
					fmt.Sprintf("%d", payload.OrderID),
					payload.Contents,
				}}, nil
			},
		)
	}

	output := t.TempDir()

	options.Store = &LocalFSStore[testOrderDestKey]{
		Resolver: func(dk testOrderDestKey) (string, error) {
			return filepath.Join(output, dk.CustomerName), nil
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	capture := options.Build()

	if err = capture.Run(ctx); err != nil {
		if err == context.Canceled || err == context.DeadlineExceeded {
			err = nil
		}
	}
	assert.Nil(err)

	s.Shutdown()

	log.Infof("output: %s", output)

	root := os.DirFS(output)

	fileCount := 0
	rowCount := 0

	for c := 'a'; c <= 'z'; c++ {
		customerDir, err := fs.Sub(root, string(c))
		assert.Nil(err)
		assert.Nil(fs.WalkDir(customerDir, ".", func(path string, d fs.DirEntry, err error) error {
			if !d.IsDir() {
				fileCount++
				assert.True(strings.HasSuffix(path, "."+options.Suffix))
				fi, err := d.Info()
				assert.Nil(err)
				assert.NotEqualValues(0, fi.Size())

				f, err := customerDir.Open(path)
				assert.Nil(err)

				r := csv.NewReader(f)

				header, err := r.Read()
				assert.Nil(err)

				assert.EqualValues(csvHeader, header)

				row1, err := r.Read()
				assert.Nil(err)

				rowCount += 1

				assert.Len(row1, 4)

				// customer name
				assert.Len(row1[0], 1)

				// timestamp
				ts, err := time.Parse(time.RFC3339, row1[1])
				assert.Nil(err)
				assert.True(ts.Year() >= 2022)

				// order id
				oid, err := strconv.Atoi(row1[2])
				assert.Nil(err)
				assert.True(oid >= cfg.startingOrderID)

				// contents
				assert.True(validThing(row1[3]))

				rows, err := r.ReadAll()
				assert.Nil(err)

				rowCount += len(rows)

				assert.Nil(f.Close())
			}
			return nil
		}))
	}

	const expectedErrors = 1

	assert.Equal(26, fileCount)
	assert.Equal(cfg.messages-expectedErrors, rowCount)
	assert.Equal(capture.fetched-capture.acked, expectedErrors)
}
