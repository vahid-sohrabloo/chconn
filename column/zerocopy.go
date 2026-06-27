package column

import "fmt"

// ZeroCopyColumn is implemented by columns that can be populated for `num` rows
// by aliasing an externally-owned byte buffer (e.g. an mmap) instead of copying
// it onto the heap. On amd64/arm64 the column's data aliases `data` directly
// (no allocation); on other CPUs it copies (and byte-swaps on big-endian).
//
// The buffer MUST outlive the column, and the column becomes READ-ONLY: any
// mutating operation is unsupported on an aliased column — `SetAt`, `Append`
// that grows beyond the aliased length, `Delete`, `DeleteFunc`, and batch-delete
// all write in place and would corrupt or fault (SIGSEGV on a read-only mmap)
// the backing buffer. Returns the number of bytes consumed from `data`.
type ZeroCopyColumn interface {
	ReadFromBytes(num int, data []byte) (consumed int, err error)
}

// ReadFromBytes implements ZeroCopyColumn for fixed-size columns.
func (c *Base[T]) ReadFromBytes(num int, data []byte) (int, error) {
	if num < 0 {
		return 0, fmt.Errorf("column %q: ReadFromBytes: num must be >= 0, got %d", c.columnHeader.Name, num)
	}
	if c.size == 0 {
		return 0, fmt.Errorf("column %q: ReadFromBytes: zero element size", c.columnHeader.Name)
	}
	if num > len(data)/c.size {
		return 0, fmt.Errorf("column %q: ReadFromBytes needs %d rows * %d bytes, have %d", c.columnHeader.Name, num, c.size, len(data))
	}
	need := num * c.size
	if len(data) < need {
		return 0, fmt.Errorf("column %q: ReadFromBytes needs %d bytes, have %d",
			c.columnHeader.Name, need, len(data))
	}
	c.Reset()
	c.numRow = num
	if num > 0 {
		c.readFromBytesAlias(num, data[:need])
	}
	return need, nil
}
