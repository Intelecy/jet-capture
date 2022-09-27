package jetcapture

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type testDestKey struct {
	K1 string
	K2 string
}

func TestFSStore(t *testing.T) {
	assert := require.New(t)

	tmp := t.TempDir()

	defer func() {
		assert.Nil(os.RemoveAll(tmp))
	}()

	s := &LocalFSStore[testDestKey]{
		Resolver: func(dk testDestKey) (string, error) {
			return filepath.Join(tmp, dk.K1, dk.K2), nil
		},
	}

	ctx := context.Background()

	testString := fmt.Sprintf("hello %s", time.Now())

	var buf bytes.Buffer
	buf.WriteString(testString)

	p, err := s.Write(ctx, &buf, testDestKey{
		K1: "k1",
		K2: "k2",
	}, "foo", "some-file-name")

	assert.Nil(err)
	assert.Equal(filepath.Join(tmp, "k1", "k2", "foo", "some-file-name"), p)

	b, err := os.ReadFile(p)
	assert.Nil(err)

	assert.EqualValues(testString, b)
}
