package jetcapture

import (
	"bytes"
	"io"
	"os"
)

type buffer interface {
	io.ReadWriter
	// TODO(jonathan): does `WriterTo` matter?
	// io.WriterTo
	DoneWriting() error
	Remove() error
}

var (
	_ buffer = &memoryBuffer{}
	_ buffer = &diskBuffer{}
	_ buffer = &wrappedWriter{}
)

type memoryBuffer struct {
	*bytes.Buffer
}

func newMemoryBuffer() buffer {
	return &memoryBuffer{
		Buffer: &bytes.Buffer{},
	}
}

func (m *memoryBuffer) DoneWriting() error { return nil }
func (m *memoryBuffer) Remove() error {
	m.Reset()
	return nil
}

type diskBuffer struct {
	*os.File
}

func (d *diskBuffer) Remove() error {
	return os.Remove(d.Name())
}

func (d *diskBuffer) DoneWriting() error {
	if err := d.File.Sync(); err != nil {
		return err
	}

	_, err := d.File.Seek(0, io.SeekStart)
	return err
}

func newDiskBuffer(tmpRoot string) (buffer, error) {
	f, err := os.CreateTemp(tmpRoot, "capture-*")
	if err != nil {
		return nil, err
	}
	log.Debugf("created %s", f.Name())
	return &diskBuffer{File: f}, nil
}

// wrappedWriter wraps an underlying buffer. it also _closes_ the "top" writer upon a call to `DoneWriting`
type wrappedWriter struct {
	buffer
	wr io.WriteCloser
}

func (w *wrappedWriter) Write(p []byte) (n int, err error) {
	return w.wr.Write(p)
}

func (w *wrappedWriter) DoneWriting() error {
	if err := w.wr.Close(); err != nil {
		return err
	}
	return w.buffer.DoneWriting()
}
