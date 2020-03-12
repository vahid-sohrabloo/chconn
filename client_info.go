package chconn

import (
	"os/user"
)

type QureyKind uint8

const (
	// Uninitialized object.
	QureyKindNoQuery      QureyKind = 0
	QureyKindInitialQuery QureyKind = 1
	// Query that was initiated by another query for distributed or ON CLUSTER query execution.
	QureyKindSecondaryQuery QureyKind = 2
)

// ClientInfo Information about client for query.
// Some fields are passed explicitly from client and some are calculated automatically.
// Contains info about initial query source, for tracing distributed queries
//  (where one query initiates many other queries).
type ClientInfo struct {
	QueryKind QureyKind
	// Current values are not serialized, because it is passed separately.
	CurrentUser    string
	CurrentQueryID string
	//todo
	// current_address;
	/// Use current user and password when sending query to replica leader
	CurrentPassword string

	// When query_kind == INITIAL_QUERY, these values are equal to current.
	InitialUser    string
	InitialQueryID string
	//todo
	//    initial_address;

	// All below are parameters related to initial query.

	// For tcp
	OSUser         string
	ClientHostname string
	ClientName     string

	ClientVersionMajor uint64
	ClientVersionMinor uint64
	ClientVersionPatch uint64
	ClientRevision     uint64

	// Common
	QuotaKey string
}

//IsEmpty check ClientInfo is empty
func (c *ClientInfo) IsEmpty() bool {
	return c.QueryKind == QureyKindNoQuery
}

// Write Only values that are not calculated automatically or passed separately are serialized.
// Revisions are passed to use format that server will understand or client was used.
func (c *ClientInfo) Write(ch *Conn) error {
	ch.writer.Uint8(uint8(c.QueryKind))
	if c.IsEmpty() {
		return nil
	}
	ch.writer.String(c.InitialUser)
	ch.writer.String(c.InitialQueryID)

	// todo
	ch.writer.String("[::ffff:127.0.0.1]:0")

	ch.writer.Uint8(1) // tcp
	ch.writer.String(c.OSUser)
	ch.writer.String(c.ClientHostname)
	ch.writer.String(c.ClientName)
	ch.writer.Uvarint(c.ClientVersionMajor)
	ch.writer.Uvarint(c.ClientVersionMinor)
	ch.writer.Uvarint(c.ClientRevision)
	if ch.ServerInfo.Revision >= DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO {
		ch.writer.String(c.QuotaKey)
	}
	if ch.ServerInfo.Revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH {
		ch.writer.Uvarint(c.ClientVersionPatch)
	}
	return nil
}

func (c *ClientInfo) fillOSUserHostNameAndVersionInfo() {
	u, err := user.Current()
	if err == nil {
		c.OSUser = u.Username
	}

	c.ClientVersionMajor = DBMS_VERSION_MAJOR
	c.ClientVersionMinor = DBMS_VERSION_MINOR
	c.ClientVersionPatch = DBMS_VERSION_PATCH
	c.ClientRevision = DBMS_VERSION_REVISION
}
