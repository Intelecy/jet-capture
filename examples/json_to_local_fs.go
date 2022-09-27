package examples

import (
	"encoding/json"
	"path/filepath"
	"time"

	"github.com/Intelecy/jet-capture"
	"github.com/nats-io/nats.go"
)

// ExamplePayload is our explicit struct for the NATS messages
type ExamplePayload struct {
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	Region    string `json:"region"`
}

// ExampleDestKey is just a simple string alias
type ExampleDestKey = string

// JSONToLocalFsCSV is an example configuration that will decode JSON messages that sent over NATS, write them out into
// CSV files, and group the output into `region` specific folder on the local file system.
//
// Use a pointer to an ExamplePayload as the Payload type parameter and ExampleDestKey as the DestKey type parameter
var JSONToLocalFsCSV = &jetcapture.Options[*ExamplePayload, ExampleDestKey]{
	Compression: jetcapture.GZip, // use gzip compression
	Suffix:      "csv",           // suffix will end up being `.csv.gz`
	MaxAge:      time.Hour,       // messages will be written once an hour

	// configure the decoder
	// the incoming NATS messages contain a JSON string which we will decode
	// we also need to return a `DestKey` which we've defined to by a string
	// this key returned is the _region_ field of the decoded message
	MessageDecoder: func(msg *nats.Msg) (*ExamplePayload, ExampleDestKey, error) {
		var p ExamplePayload
		if err := json.Unmarshal(msg.Data, &p); err != nil {
			return nil, "", err
		}
		return &p, p.Region, nil
	},

	// use the jetcapture.NewCSVWriter helper
	// we need to specify the headers, and a function that will "flatten" the payload
	// into one or more CSV rows
	WriterFactory: func() jetcapture.FormattedDataWriter[*ExamplePayload] {
		return jetcapture.NewCSVWriter(
			[]string{"first_name", "last_name", "region"},
			func(p *ExamplePayload) ([][]string, error) {
				return [][]string{{
					p.FirstName,
					p.LastName,
					p.Region,
				}}, nil
			},
		)
	},

	// use the jetcapture.LocalFSStore helper
	// we need to provide a `Resolver` that returns a filesystem path using the destination key
	// the path will use the `region` field to group output
	Store: &jetcapture.LocalFSStore[ExampleDestKey]{
		Resolver: func(dk ExampleDestKey) (string, error) {
			return filepath.Join("backup", dk), nil
		},
	},
}
