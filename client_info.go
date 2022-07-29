package chconn

import (
	"os/user"
)

// ClientInfo Information about client for query.
// Some fields are passed explicitly from client and some are calculated automatically.
// Contains info about initial query source, for tracing distributed queries
// where one query initiates many other queries.
type ClientInfo struct {
	InitialUser    string
	InitialQueryID string

	OSUser         string
	ClientHostname string
	ClientName     string

	ClientVersionMajor uint64
	ClientVersionMinor uint64
	ClientVersionPatch uint64
	ClientRevision     uint64
	DistributedDepth   uint64

	QuotaKey string
}

// Write Only values that are not calculated automatically or passed separately are serialized.
// Revisions are passed to use format that server will understand or client was used.
func (c *ClientInfo) write(ch *conn) {
	// InitialQuery
	ch.writer.Uint8(1)

	ch.writer.String(c.InitialUser)
	ch.writer.String(c.InitialQueryID)

	ch.writer.String("[::ffff:127.0.0.1]:0")

	if ch.serverInfo.Revision >= dbmsMinProtocolVersionWithInitialQueryStartTime {
		ch.writer.Uint64(0)
	}

	// iface type
	ch.writer.Uint8(1) // tcp
	ch.writer.String(c.OSUser)
	ch.writer.String(c.ClientHostname)
	ch.writer.String(c.ClientName)
	ch.writer.Uvarint(c.ClientVersionMajor)
	ch.writer.Uvarint(c.ClientVersionMinor)
	ch.writer.Uvarint(c.ClientRevision)

	if ch.serverInfo.Revision >= dbmsMinRevisionWithQuotaKeyInClientInfo {
		ch.writer.String(c.QuotaKey)
	}

	if ch.serverInfo.Revision >= dbmsMinProtocolVersionWithDistributedDepth {
		ch.writer.Uvarint(c.DistributedDepth)
	}

	if ch.serverInfo.Revision >= dbmsMinRevisionWithVersionPatch {
		ch.writer.Uvarint(c.ClientVersionPatch)
	}

	if ch.serverInfo.Revision >= dbmsMinRevisionWithOpenTelemetry {
		ch.writer.Uint8(0)
	}

	if ch.serverInfo.Revision >= dbmsMinProtocolVersionWithParallelReplicas {
		ch.writer.Uvarint(0) // collaborate_with_initiator
		ch.writer.Uvarint(0) // count_participating_replicas
		ch.writer.Uvarint(0) // number_of_current_replica
	}
}

func (c *ClientInfo) fillOSUserHostNameAndVersionInfo() {
	u, err := user.Current()
	if err == nil {
		c.OSUser = u.Username
	}

	c.ClientVersionMajor = dbmsVersionMajor
	c.ClientVersionMinor = dbmsVersionMinor
	c.ClientVersionPatch = dbmsVersionPatch
	c.ClientRevision = dbmsVersionRevision
}
