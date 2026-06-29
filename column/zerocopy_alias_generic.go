//go:build !amd64 && !arm64

package column

import (
	"unsafe"

	"github.com/vahid-sohrabloo/chconn/v3/internal/helper"
)

// zeroCopyLittleEndian reports the CPU byte order once at init.
var zeroCopyLittleEndian = func() bool {
	var x uint16 = 1
	return *(*byte)(unsafe.Pointer(&x)) == 1
}()

// readFromBytesAlias copies into an aligned heap slice (avoiding unaligned loads
// on non-amd64/arm64 arches) and byte-swaps per element on big-endian CPUs.
func (c *Base[T]) readFromBytesAlias(num int, data []byte) {
	if num == 0 {
		c.values = nil
		return
	}
	c.values = helper.ResetSlice(c.values, num, true)
	dst := helper.ConvertToByte(c.values, c.size)
	copy(dst, data)
	if !zeroCopyLittleEndian && c.size > 1 {
		for i := 0; i < len(dst); i += c.size {
			for l, r := i, i+c.size-1; l < r; l, r = l+1, r-1 {
				dst[l], dst[r] = dst[r], dst[l]
			}
		}
	}
}
