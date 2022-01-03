package column

import (
	"io"
)

type nullable struct {
	column
}

func newNullable() *nullable {
	return &nullable{
		column: column{
			size: Uint8Size,
		},
	}
}

func (c *nullable) Append(v uint8) {
	c.writerData = append(c.writerData,
		v,
	)
}

func (c *nullable) WriteTo(w io.Writer) (int64, error) {
	nw, err := w.Write(c.writerData)
	c.reset()
	return int64(nw), err
}
