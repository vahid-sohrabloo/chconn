package chconn

import "github.com/vahid-sohrabloo/chconn/v3/internal/helper"

// Progress details of progress select query
type Progress struct {
	ReadRows     uint64
	ReadBytes    uint64
	TotalRows    uint64
	TotalBytes   uint64
	WriterRows   uint64
	WrittenBytes uint64
	ElapsedNS    uint64
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

	if ch.serverInfo.Revision >= helper.DbmsMinProtocolVersionWithTotalBytesInProgress {
		if p.TotalBytes, err = ch.reader.Uvarint(); err != nil {
			return &readError{"progress: read TotalBytes", err}
		}
	}

	if ch.serverInfo.Revision >= helper.DbmsMinRevisionWithClientWriteInfo {
		if p.WriterRows, err = ch.reader.Uvarint(); err != nil {
			return &readError{"progress: read WriterRows", err}
		}
		if p.WrittenBytes, err = ch.reader.Uvarint(); err != nil {
			return &readError{"progress: read WrittenBytes", err}
		}
	}
	if ch.serverInfo.Revision >= helper.DbmsMinProtocolWithServerQueryTimeInProgress {
		if p.ElapsedNS, err = ch.reader.Uvarint(); err != nil {
			return &readError{"progress: read ElapsedNS", err}
		}
	}

	return nil
}
