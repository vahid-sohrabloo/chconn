package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUUID(t *testing.T) {
	be := [16]byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x10}
	uuidData := UUIDFromBigEndian(be)
	assert.Equal(t, be, uuidData.BigEndian())
	assert.Equal(t, "01234567-89ab-cdef-fedc-ba9876543210", string(uuidData.Append([]byte{})))
	assert.Equal(t, "x=01234567-89ab-cdef-fedc-ba9876543210", string(uuidData.Append([]byte("x="))))
}
