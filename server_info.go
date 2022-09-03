package chconn

import (
	"fmt"

	"github.com/vahid-sohrabloo/chconn/v2/internal/helper"
	"github.com/vahid-sohrabloo/chconn/v2/internal/readerwriter"
)

// ServerInfo detail of server info
type ServerInfo struct {
	Name               string
	Revision           uint64
	MinorVersion       uint64
	MajorVersion       uint64
	ServerDisplayName  string
	ServerVersionPatch uint64
	Timezone           string
}

func (srv *ServerInfo) read(r *readerwriter.Reader) (err error) {
	if srv.Name, err = r.String(); err != nil {
		return &readError{"ServerInfo: could not read server name", err}
	}
	if srv.MajorVersion, err = r.Uvarint(); err != nil {
		return &readError{"ServerInfo: could not read server major version", err}
	}
	if srv.MinorVersion, err = r.Uvarint(); err != nil {
		return &readError{"ServerInfo: could not read server minor version", err}
	}
	if srv.Revision, err = r.Uvarint(); err != nil {
		return &readError{"ServerInfo: could not read server revision", err}
	}
	if srv.Revision >= helper.DbmsMinRevisionWithServerTimezone {
		if srv.Timezone, err = r.String(); err != nil {
			return &readError{"ServerInfo: could not read server timezone", err}
		}
	}
	if srv.Revision >= helper.DbmsMinRevisionWithServerDisplayName {
		if srv.ServerDisplayName, err = r.String(); err != nil {
			return &readError{"ServerInfo: could not read server display name", err}
		}
	}
	if srv.Revision >= helper.DbmsMinRevisionWithVersionPatch {
		if srv.ServerVersionPatch, err = r.Uvarint(); err != nil {
			return &readError{"ServerInfo: could not read server version patch", err}
		}
	}
	return nil
}

func (srv *ServerInfo) String() string {
	return fmt.Sprintf("%s %d.%d.%d (%s) %s %d",
		srv.Name,
		srv.MajorVersion,
		srv.MinorVersion,
		srv.Revision,
		srv.Timezone,
		srv.ServerDisplayName,
		srv.ServerVersionPatch)
}

// ServerInfo get server info
func (ch *conn) ServerInfo() *ServerInfo {
	return ch.serverInfo
}
