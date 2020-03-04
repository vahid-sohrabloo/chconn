package chconn

import (
	"fmt"
	"time"
)

type ServerInfo struct {
	Name               string
	Revision           uint64
	MinorVersion       uint64
	MajorVersion       uint64
	ServerDisplayName  string
	ServerVersionPatch uint64
	Timezone           *time.Location
}

func (srv *ServerInfo) Read(r *Reader) (err error) {
	if srv.Name, err = r.String(); err != nil {
		return fmt.Errorf("could not read server name: %v", err)
	}
	if srv.MajorVersion, err = r.Uvarint(); err != nil {
		return fmt.Errorf("could not read server major version: %v", err)
	}
	if srv.MinorVersion, err = r.Uvarint(); err != nil {
		return fmt.Errorf("could not read server minor version: %v", err)
	}
	if srv.Revision, err = r.Uvarint(); err != nil {
		return fmt.Errorf("could not read server revision: %v", err)
	}
	if srv.Revision >= DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE {
		var timezone string
		timezone, err = r.String()
		if err != nil {
			return fmt.Errorf("could not read server timezone: %v", err)
		}
		if srv.Timezone, err = time.LoadLocation(timezone); err != nil {
			return fmt.Errorf("could not load time location: %v", err)
		}
	}
	if srv.Revision >= DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME {
		if srv.ServerDisplayName, err = r.String(); err != nil {
			return fmt.Errorf("could not read server name: %v", err)
		}
	}
	if srv.Revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH {
		if srv.ServerVersionPatch, err = r.Uvarint(); err != nil {
			return fmt.Errorf("could not read server major version: %v", err)
		}
	}
	return nil
}

func (srv ServerInfo) String() string {
	return fmt.Sprintf("%s %d.%d.%d (%s) %s %d",
		srv.Name,
		srv.MajorVersion,
		srv.MinorVersion,
		srv.Revision,
		srv.Timezone,
		srv.ServerDisplayName,
		srv.ServerVersionPatch)
}
