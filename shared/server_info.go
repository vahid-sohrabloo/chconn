package shared

import (
	"fmt"
	"time"
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
	PasswordPatterns   []ServerInfoPasswordRules
}

// ServerInfoPasswordRules contains a regex pattern and message for server-side password validation.
type ServerInfoPasswordRules struct {
	Pattern string
	Message string
}

func (srv *ServerInfo) String() string {
	return fmt.Sprintf("%s %d.%d.%d (%s) %s %d",
		srv.Name,
		srv.MajorVersion,
		srv.MinorVersion,
		srv.Revision,
		srv.Timezone,
		srv.ServerDisplayName,
		srv.ServerVersionPatch,
	)
}

// EmptyServerInfo returns a [ServerInfo] with zero values and the local timezone.
func EmptyServerInfo() *ServerInfo {
	return &ServerInfo{
		Name:               "",
		Revision:           0,
		MajorVersion:       0,
		MinorVersion:       0,
		ServerDisplayName:  "",
		ServerVersionPatch: 0,
		Timezone:           time.Local.String(),
	}
}
