# NATS JetStream Capture

> Note: requires Go 1.19 or later

## Overview

**jetcapture** is a library for building reliable and continuous [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream)
backup processes.

* **Decode**      -- take the incoming NATS message and deserialize it into something concrete
* **Split**       -- optionally split the incoming message stream and group the messages by one or more user-defined
                     attributes
* **Serialize**   -- write decoded messages to group-specific "blocks" using a provided serialization method (e.g. CSV,
                     Parquet, raw-bytes)
* **Store**       -- copy the completed blocks to a user-provided storage endpoint (e.g. local folders, Azure, etc.)

For example, you can take a stream of JSON pizza orders, group them by `pizza_store_id`, and write them out as flattened
CSV in 15 minute blocks with a separate location (e.g. folder, or S3 bucket) for each store.

**jetcapture** uses a [pull consumer](https://docs.nats.io/nats-concepts/jetstream/consumers) which means horizontal
scalability is built in. Just add more instances to increase throughput.


### Internal Data Flow

1. **jetcapture** begins pulling messages from a named consumer and stream
2. For each message:
   1. Pass the raw NATS message into the user-provided **decoder**, returning a typed struct and an optional
      "destination key"
   2. Find a corresponding "block" using the **destination key** and the message timestamp (truncated to the storage
      interval)
   3. Call the user-provided **serialize** function and write the decoded message into a block-specific temporary buffer
   4. Cache the message ack information for later
3. After each storage interval has passed, for each block:
   1. Call the user-provided **store** function to persist the block to a permanent storage location
   2. Assuming storage of the block succeeded, "ack" all the messages in the block

## Custom Implementation Requirements

> Note: **jetcapture** uses Go generics to enable strongly typed callback implementations

1. Define your `Payload P` and `DestKey K` types
2. Implement a `MessageDecoder` that takes a `*nats.Msg` and returns a decoded message of type `P` and a "destination
   key" of type `K`
3. Implement a `FormattedDataWriter[P Payload]` which takes a payload `P` "writes" it to an underlying `io.Writer`. Or,
   use a helper writer like `CSVWriter[P Payload]` or `NewLineDelimitedJSON[P Payload]`
4. Implement a `BlockStore[K DestKey]` which can write out the finalized "block" (exposed as `io.Reader`). Or, use a
   helper like `LocalFSStore[K DestKey]` or `AzureBlobStore[K DestKey]`
5. Create a typed `jetcapture.Options[P, K]` instance with options set
6. Connect to a NATS server
7. Call `options.Build().Run(ctx, natsConn)`

For a full example see the [sample application](apps/ndjson/main.go) that takes incoming NATS messages, encodes the entire message itself as
JSON, and writes it out using newline-delimited JSON.

For an example of a custom decoder (which most libary users will need), see the example below

### Types

```golang
// Payload is a type that represents your deserialized NATS message
type Payload interface {
	any
}

// DestKey is a type that represents how you want to group (i.e. split) your messages.
type DestKey interface {
	comparable
}
```

### Implement

```golang
type FormattedDataWriter[P Payload] interface {
	InitNew(out io.Writer) error
	Write(payload P) (int, error)
	Flush() error
}

type BlockStore[K DestKey] interface {
	// Write takes a block and writes it out to a "final" destination as specified
	// by the deskKey, dir and file name. returns a stringified version of the
	// destination
	Write(ctx context.Context, block io.Reader, destKey K, dir, fileName string) (string, error)
}

type Options[P Payload, K DestKey] struct {
	...
	MessageDecoder func(*nats.Msg) (P, K, error)
	WriterFactory  func() FormattedDataWriter[P]
	Store          BlockStore[K]
}
```


## Example

```golang
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
```

## TODO

- [ ] Decide on explicit `nack` strategy where possible
- [ ] Add S3 store example
- [ ] Stats export
- [ ] Add `DrainTimeout` for `Capture.sweepBlocks`. Right now a canceled context (e.g. CTRL-C) triggers a final sweep.
      However, for calls that _take_ a context during a `BlockStore.Write` call (e.g. Azure blob store), the call will
      often be short-circuited. A separate drain/sweep context should be created with a timeout.
- [ ] Add better logging configuration/interface
- [ ] Add support for checking outstanding acks and warning if near or at limit
- [ ] Investigate a Go routine pool for `BlockStore.Write` (current code blocks during the write phase)
- [ ] Output filenames need some more thought

## Credits

* Jonathan Camp @intelecy