package jetcapture

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"os"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

var (
	_ BlockStore[string] = &AzureBlobStore[string]{}
)

type BuildURLBase[K DestKey] func(destKey K) (string, error)

type AzureBlobStore[K DestKey] struct {
	credz        *azidentity.DefaultAzureCredential
	buildURLBase BuildURLBase[K]
}

func NewAzureBlobStore[K DestKey](buildURLBase BuildURLBase[K]) (*AzureBlobStore[K], error) {
	credential, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, err
	}

	return &AzureBlobStore[K]{
		buildURLBase: buildURLBase,
		credz:        credential,
	}, nil
}

func (a *AzureBlobStore[K]) Write(ctx context.Context, block io.Reader, destKey K, dir, fileName string) (string, error) {
	base, err := a.buildURLBase(destKey)
	if err != nil {
		return "", err
	}

	u, err := url.JoinPath(base, dir, fileName)
	if err != nil {
		return "", err
	}

	bc, err := azblob.NewBlockBlobClient(u, a.credz, nil)
	if err != nil {
		return "", err
	}

	var options azblob.UploadStreamOptions

	resp, err := bc.UploadStream(ctx, block, options)
	if err != nil {
		return "", err
	}

	if resp.RawResponse.StatusCode != http.StatusOK {
		_, _ = io.Copy(os.Stderr, resp.RawResponse.Body)
	}

	return u, nil
}
