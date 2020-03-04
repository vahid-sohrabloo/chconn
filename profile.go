package chconn

type Profile struct {
	Rows                      uint64
	Blocks                    uint64
	Bytes                     uint64
	RowsBeforeLimit           uint64
	AppliedLimit              bool
	CalculatedRowsBeforeLimit bool
}

func NewProfile() *Profile {
	return &Profile{}
}

func (p *Profile) Read(ch *Conn) (err error) {
	if p.Rows, err = ch.reader.Uvarint(); err != nil {
		return err
	}
	if p.Blocks, err = ch.reader.Uvarint(); err != nil {
		return err
	}
	if p.Bytes, err = ch.reader.Uvarint(); err != nil {
		return err
	}

	if p.AppliedLimit, err = ch.reader.Bool(); err != nil {
		return err
	}
	if p.RowsBeforeLimit, err = ch.reader.Uvarint(); err != nil {
		return err
	}
	if p.CalculatedRowsBeforeLimit, err = ch.reader.Bool(); err != nil {
		return err
	}
	return nil

}
