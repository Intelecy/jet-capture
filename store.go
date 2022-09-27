package jetcapture

import (
	"context"
	"io"
	"os"
	"path"
)

type BlockStore[K DestKey] interface {
	// Write takes a block and writes it out to a "final" destination as specified by the deskKey, dir and file name.
	// returns a stringified version of the destination
	// TODO(jonathan): should the block be an io.ReadCloser?
	Write(ctx context.Context, block io.Reader, destKey K, dir, fileName string) (string, error)
}

var (
	_ BlockStore[string] = &LocalFSStore[string]{}
)

type LocalFSStore[K DestKey] struct {
	Resolver func(destKey K) (string, error)
}

func (f *LocalFSStore[K]) Write(_ context.Context, block io.Reader, destKey K, dir, fileName string) (string, error) {
	p, err := f.Resolver(destKey)
	if err != nil {
		return "", err
	}

	p = path.Join(p, dir)

	if err := os.MkdirAll(p, 0755); err != nil {
		return "", err
	}

	p = path.Join(p, fileName)

	log.Debugf("writing block to %s", p)

	fout, err := os.Create(p)
	if err != nil {
		return p, err
	}

	defer fout.Close()

	_, err = io.Copy(fout, block)
	if err != nil {
		return p, err
	}

	return p, fout.Close()
}

func SingleDirStore[K DestKey](path string) BlockStore[K] {
	return &LocalFSStore[K]{
		Resolver: func(K) (string, error) {
			// do nothing with K and just return a static path
			return path, nil
		},
	}
}
