package jetcapture

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"os"

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

type AzureBlobStore[K DestKey] struct {
	credz        azcore.TokenCredential
	buildURLBase BuildURLBase[K]
}

func NewAzureBlobStore[K DestKey](
	credential azcore.TokenCredential,
	buildURLBase BuildURLBase[K],
) (*AzureBlobStore[K], error) {
	return &AzureBlobStore[K]{
		buildURLBase: buildURLBase,
		credz:        credential,
	}, nil
}

func (a *AzureBlobStore[K]) Write(ctx context.Context, block io.Reader, destKey K, dir, fileName string) (string, error) {
	base, err := a.buildURLBase(ctx, destKey)
	if err != nil {
		return "", err
	}

	u, err := url.JoinPath(base, dir, fileName)
	if err != nil {
		return "", err
	}

	log.Infof("writing block to %s", u)

	bc, err := azblob.NewBlockBlobClient(u, a.credz, nil)
	if err != nil {
		return "", err
	}

	var options azblob.UploadStreamOptions

	options.BufferSize = azureUploadBufferSz

	resp, err := bc.UploadStream(ctx, block, options)
	if err != nil {
		return "", err
	}

	if resp.RawResponse.StatusCode != http.StatusOK {
		_, _ = io.Copy(os.Stderr, resp.RawResponse.Body)
	}

	return u, nil
}
