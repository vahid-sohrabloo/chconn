//go:build amd64 || arm64

package column

import "unsafe"

// readFromBytesAlias points values at data with no copy (fast, unaligned-safe arches).
func (c *Base[T]) readFromBytesAlias(num int, data []byte) {
	if num == 0 {
		c.values = nil
		return
	}
	c.values = unsafe.Slice((*T)(unsafe.Pointer(&data[0])), num)
}
