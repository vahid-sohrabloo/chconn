package helper

import "unsafe"

func ConvertToByte[T any](v []T, size int) []byte {
	if len(v) == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(&v[0])),
		len(v)*size)
}
