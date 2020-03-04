package chconn

type Progress struct {
	readRows     uint64
	readbytes    uint64
	totalRows    uint64
	writerRows   uint64
	writtenBytes uint64
}

func NewProgress() *Progress {
	return &Progress{}
}

func (p *Progress) Read(ch *Conn) (err error) {

	if p.readRows, err = ch.reader.Uvarint(); err != nil {
		return err
	}
	if p.readbytes, err = ch.reader.Uvarint(); err != nil {
		return err
	}

	if p.totalRows, err = ch.reader.Uvarint(); err != nil {
		return err
	}

	if ch.ServerInfo.Revision >= DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO {
		if p.writerRows, err = ch.reader.Uvarint(); err != nil {
			return err
		}
		if p.writtenBytes, err = ch.reader.Uvarint(); err != nil {
			return err
		}
	}

	return nil
}
