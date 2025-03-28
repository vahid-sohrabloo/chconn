package shared

import (
	"fmt"
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
