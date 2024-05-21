package chconn

import (
	"fmt"

	"github.com/vahid-sohrabloo/chconn/v3/internal/helper"
	"github.com/vahid-sohrabloo/chconn/v3/internal/readerwriter"
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
	if srv.Revision >= helper.DbmsMinProtocolVersionWithPasswordComplexityRules {
		lenRules, err := r.Uvarint()
		if err != nil {
			return &readError{"ServerInfo: could not read server password complexity rules: len", err}
		}
		srv.PasswordPatterns = make([]ServerInfoPasswordRules, lenRules)
		for i := uint64(0); i < lenRules; i++ {
			var rule ServerInfoPasswordRules
			if rule.Pattern, err = r.String(); err != nil {
				return &readError{"ServerInfo: could not read server password complexity rules: pattern", err}
			}
			if rule.Message, err = r.String(); err != nil {
				return &readError{"ServerInfo: could not read server password complexity rules: pattern", err}
			}
			srv.PasswordPatterns[i] = rule
		}
	}

	if srv.Revision >= helper.DbmsMinRevisionWithInterserverSecretV2 {
		// read secret nonce
		// we don't need it for now
		_, err := r.Uint64()
		if err != nil {
			return &readError{"ServerInfo: could not read server interserver secret nonce", err}
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
