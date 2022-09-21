package jetcapture

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

type testPayload struct {
	A string
	B int
	C bool
}

func runWriter[P Payload](assert *require.Assertions, w FormattedDataWriter[P], payloads []P) []byte {
	var buf bytes.Buffer

	assert.Nil(w.InitNew(&buf))

	for _, p := range payloads {
		_, err := w.Write(p)
		assert.Nil(err)
	}

	assert.Nil(w.Flush())

	return buf.Bytes()
}

func TestNewLineDelimitedJSON(t *testing.T) {
	assert := require.New(t)

	buf := runWriter[testPayload](assert, &NewLineDelimitedJSON[testPayload]{}, nil)
	assert.Len(buf, 0)

	buf = runWriter[testPayload](assert, &NewLineDelimitedJSON[testPayload]{}, []testPayload{{
		A: "hello",
		B: 1337,
		C: true,
	}})

	assert.Equal("{\"A\":\"hello\",\"B\":1337,\"C\":true}\n", string(buf))
}

func TestCSVWriter(t *testing.T) {
	assert := require.New(t)

	newWriter := func() FormattedDataWriter[testPayload] {
		return NewCSVWriter[testPayload]([]string{"a", "b", "c"}, func(p testPayload) ([][]string, error) {
			return [][]string{[]string{
				p.A,
				fmt.Sprintf("%d", p.B),
				fmt.Sprintf("%v", p.C),
			}}, nil
		})
	}

	buf := runWriter[testPayload](assert, newWriter(), nil)
	assert.Equal("a,b,c\n", string(buf))

	buf = runWriter[testPayload](assert, newWriter(), []testPayload{{
		A: "hello",
		B: 1337,
		C: true,
	}})
	assert.Equal("a,b,c\nhello,1337,true\n", string(buf))
}
