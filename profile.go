package chconn

// Profile detail of profile select query
type Profile struct {
	Rows                      uint64
	Blocks                    uint64
	Bytes                     uint64
	RowsBeforeLimit           uint64
	AppliedLimit              bool
	CalculatedRowsBeforeLimit bool
}

func newProfile() *Profile {
	return &Profile{}
}

func (p *Profile) read(ch *conn) (err error) {
	if p.Rows, err = ch.reader.Uvarint(); err != nil {
		return &readError{"profile: read Rows", err}
	}
	if p.Blocks, err = ch.reader.Uvarint(); err != nil {
		return &readError{"profile: read Blocks", err}
	}
	if p.Bytes, err = ch.reader.Uvarint(); err != nil {
		return &readError{"profile: read Bytes", err}
	}

	if p.AppliedLimit, err = ch.reader.Bool(); err != nil {
		return &readError{"profile: read AppliedLimit", err}
	}
	if p.RowsBeforeLimit, err = ch.reader.Uvarint(); err != nil {
		return &readError{"profile: read RowsBeforeLimit", err}
	}
	if p.CalculatedRowsBeforeLimit, err = ch.reader.Bool(); err != nil {
		return &readError{"profile: read CalculatedRowsBeforeLimit", err}
	}
	return nil
}
