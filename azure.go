package jetcapture

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

const azureUploadBufferSz = 64 * 1024 * 1024

var (
	_ BlockStore[string] = &AzureBlobStore[string]{}
)

// BuildURLBase should return a URL that serves as the base for the block
// For example: https://capture.blob.core.windows.net/backup/from-stream-foo/
type BuildURLBase[K DestKey] func(ctx context.Context, destKey K) (string, error)

// OverrideUploadOptions is an optional function to override various upload option (e.g. AccessTier)
type OverrideUploadOptions[K DestKey] func(options *azblob.UploadStreamOptions, destKey K)

type AzureBlobStore[K DestKey] struct {
	credz          azcore.TokenCredential
	buildURLBaseFn BuildURLBase[K]
	optionsFn      OverrideUploadOptions[K]
}

func NewAzureBlobStore[K DestKey](
	credential azcore.TokenCredential,
	buildURLBaseFn BuildURLBase[K],
	optionsFn OverrideUploadOptions[K],

) (*AzureBlobStore[K], error) {
	return &AzureBlobStore[K]{
		buildURLBaseFn: buildURLBaseFn,
		optionsFn:      optionsFn,
		credz:          credential,
	}, nil
}

func (a *AzureBlobStore[K]) Write(ctx context.Context, block io.Reader, destKey K, dir, fileName string) (string, int64, time.Duration, error) {
	start := time.Now()

	base, err := a.buildURLBaseFn(ctx, destKey)
	if err != nil {
		return "", 0, 0, err
	}

	u, err := url.JoinPath(base, dir, fileName)
	if err != nil {
		return "", 0, 0, err
	}

	log.Infof("writing block to %s", u)

	bc, err := azblob.NewBlockBlobClient(u, a.credz, nil)
	if err != nil {
		return "", 0, 0, err
	}

	var options azblob.UploadStreamOptions

	if a.optionsFn != nil {
		a.optionsFn(&options, destKey)
	}

	if options.BufferSize == 0 {
		options.BufferSize = azureUploadBufferSz
	}

	reader := &countingReader{Reader: block}

	resp, err := bc.UploadStream(ctx, reader, options)
	if err != nil {
		return "", 0, 0, err
	}

	if resp.RawResponse.StatusCode != http.StatusOK {
		_, _ = io.Copy(os.Stderr, resp.RawResponse.Body)
	}

	return u, int64(reader.n), time.Since(start), nil
}

type countingReader struct {
	io.Reader
	n int
}

func (r *countingReader) Read(p []byte) (n int, err error) {
	n, err = r.Reader.Read(p)
	r.n += n
	return
}
