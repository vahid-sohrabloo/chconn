package types

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestUUID(t *testing.T) {
	u := uuid.New()
	uuidData := UUIDFromBigEndian(u)
	assert.Equal(t, uuidData.BigEndian(), [16]byte(u))
}
