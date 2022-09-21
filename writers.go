package jetcapture

import (
	"encoding/csv"
	"encoding/json"
	"io"
)

type FormattedDataWriter[P Payload] interface {
	InitNew(out io.Writer) error
	Write(payload P) (int, error)
	Flush() error
}

type NewLineDelimitedJSON[P Payload] struct {
	out io.Writer
	enc *json.Encoder
}

func (j *NewLineDelimitedJSON[P]) InitNew(out io.Writer) error {
	j.out = out
	j.enc = json.NewEncoder(out)
	return nil
}

func (j *NewLineDelimitedJSON[P]) Write(m P) (int, error) {
	return 1, j.enc.Encode(m)
}

func (j *NewLineDelimitedJSON[P]) Flush() error { return nil }

type CSVWriter[P Payload] struct {
	out     io.Writer
	csv     *csv.Writer
	header  []string
	flatten func(p P) ([][]string, error)
}

func NewCSVWriter[P Payload](
	header []string,
	flattenFn func(payload P) ([][]string, error),
) FormattedDataWriter[P] {
	return &CSVWriter[P]{
		header:  header,
		flatten: flattenFn,
	}
}

func (c *CSVWriter[P]) InitNew(out io.Writer) error {
	c.out = out
	c.csv = csv.NewWriter(c.out)
	if len(c.header) > 0 {
		return c.csv.Write(c.header)
	}
	return nil
}

func (c *CSVWriter[P]) Write(m P) (int, error) {
	rows, err := c.flatten(m)
	if err != nil {
		return 0, err
	}
	if err := c.csv.WriteAll(rows); err != nil {
		return 0, err
	}
	return len(rows), nil
}

func (c *CSVWriter[P]) Flush() error {
	c.csv.Flush()
	return c.csv.Error()
}
