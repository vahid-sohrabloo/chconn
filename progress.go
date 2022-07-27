package chconn

// Progress details of progress select query
type Progress struct {
	ReadRows     uint64
	ReadBytes    uint64
	TotalRows    uint64
	WriterRows   uint64
	WrittenBytes uint64
}

func newProgress() *Progress {
	return &Progress{}
}

func (p *Progress) read(ch *conn) (err error) {
	if p.ReadRows, err = ch.reader.Uvarint(); err != nil {
		return &readError{"progress: read ReadRows", err}
	}
	if p.ReadBytes, err = ch.reader.Uvarint(); err != nil {
		return &readError{"progress: read ReadBytes", err}
	}

	if p.TotalRows, err = ch.reader.Uvarint(); err != nil {
		return &readError{"progress: read TotalRows", err}
	}

	if ch.serverInfo.Revision >= dbmsMinRevisionWithClientWriteInfo {
		if p.WriterRows, err = ch.reader.Uvarint(); err != nil {
			return &readError{"progress: read WriterRows", err}
		}
		if p.WrittenBytes, err = ch.reader.Uvarint(); err != nil {
			return &readError{"progress: read WrittenBytes", err}
		}
	}

	return nil
}
