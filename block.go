package chconn

import (
	"bytes"
	"fmt"

	"github.com/vahid-sohrabloo/chconn/v3/column"
	"github.com/vahid-sohrabloo/chconn/v3/internal/helper"
	"github.com/vahid-sohrabloo/chconn/v3/internal/readerwriter"
	"github.com/vahid-sohrabloo/chconn/v3/shared"
)

// Column contains details of ClickHouse column

type block struct {
	c             *conn
	ColumnsHeader []column.ColumnHeader
	NumRows       uint64
	NumColumns    uint64
	info          blockInfo
	headerWriter  *readerwriter.Writer
}

func newBlock(ch *conn) *block {
	return &block{
		c:            ch,
		headerWriter: readerwriter.NewWriter(),
	}
}

func (b *block) reset() {
	b.headerWriter.Reset()
	b.ColumnsHeader = b.ColumnsHeader[:0]
	b.NumRows = 0
	b.NumColumns = 0
}

func (b *block) read() error {
	if _, err := b.c.reader.ByteString(); err != nil { // temporary table
		return &readError{"block: temporary table", err}
	}
	b.c.reader.SetCompress(b.c.compress)
	defer b.c.reader.SetCompress(false)

	var err error
	err = b.info.read(b.c.reader)
	if err != nil {
		return err
	}

	b.NumColumns, err = b.c.reader.Uvarint()
	if err != nil {
		return &readError{"block: read NumColumns", err}
	}

	b.NumRows, err = b.c.reader.Uvarint()
	if err != nil {
		return &readError{"block: read NumRows", err}
	}
	return nil
}

func readColumnHeader(r *readerwriter.Reader, serverInfo *shared.ServerInfo) (column.ColumnHeader, error) {
	var err error
	c := column.ColumnHeader{}
	c.Name, err = r.ReadBytes(c.Name)
	if err != nil {
		return c, fmt.Errorf("read column name: %w", err)
	}

	c.ChType, err = r.ReadBytes(c.ChType)
	if err != nil {
		return c, fmt.Errorf("read column type: %w", err)
	}

	if serverInfo.Revision >= helper.DbmsMinProtocolWithCustomSerialization {
		hasCustomSerialization, err := r.ReadByte()
		if err != nil {
			return c, fmt.Errorf("read custom serialization: %w", err)
		}
		if hasCustomSerialization == 1 {
			useCustomSerialization, err := r.ReadByte()
			if err != nil {
				return c, fmt.Errorf("read  has custom serialization: %w", err)
			}
			if useCustomSerialization == 1 {
				c.IsSparse = true
			}
		}
	}

	return c, nil
}

func (b *block) readColumnsHeader() error {
	b.c.reader.SetCompress(b.c.compress)
	defer b.c.reader.SetCompress(false)
	b.ColumnsHeader = make([]column.ColumnHeader, b.NumColumns)

	for i := uint64(0); i < b.NumColumns; i++ {
		col, err := readColumnHeader(b.c.reader, b.c.serverInfo)
		if err != nil {
			return err
		}
		b.ColumnsHeader[i] = col
	}
	return nil
}

func (b *block) readColumnsData(needValidateData bool, columns ...column.ColumnCore) error {
	b.c.reader.SetCompress(b.c.compress)
	defer b.c.reader.SetCompress(false)
	for _, col := range columns {
		colHeader, err := readColumnHeader(b.c.reader, b.c.serverInfo)
		if err != nil {
			return fmt.Errorf("read column header %q: %w", string(colHeader.Name), err)
		}
		err = col.SetColumnHeader(colHeader)
		if err != nil {
			return fmt.Errorf("read column header %q: %w", string(colHeader.Name), err)
		}
		err = col.ReadHeader(b.c.reader, b.c.serverInfo)
		if err != nil {
			return fmt.Errorf("read column header \"%s\": %w", string(col.Name()), err)
		}

		if b.NumRows == 0 {
			continue
		}
		err = col.ReadRaw(int(b.NumRows))
		if err != nil {
			return fmt.Errorf("read data %q: %w", col.Name(), err)
		}
	}
	return nil
}

func (b *block) reorderColumns(columns []column.ColumnCore) ([]column.ColumnCore, error) {
	for i, c := range b.ColumnsHeader {
		// check if already sorted
		if bytes.Equal(columns[i].Name(), b.ColumnsHeader[i].Name) {
			continue
		}
		index, col := findColumn(columns, c.Name)
		if col == nil {
			return nil, &ColumnNotFoundError{
				Column: string(c.Name),
			}
		}
		columns[index] = columns[i]
		columns[i] = col
	}
	return columns, nil
}

func findColumn(columns []column.ColumnCore, name []byte) (int, column.ColumnCore) {
	for i, col := range columns {
		if bytes.Equal(col.Name(), name) {
			return i, col
		}
	}
	return 0, nil
}

func (b *block) writeHeader(numRows int) error {
	b.info.write(b.c.writer)
	// NumColumns
	b.c.writer.Uvarint(b.NumColumns)
	// NumRows
	b.c.writer.Uvarint(uint64(numRows))
	_, err := b.c.writer.WriteTo(b.c.writerToCompress)
	if err != nil {
		return &writeError{"write block info", err}
	}
	err = b.c.flushCompress()
	if err != nil {
		return &writeError{"flush block info", err}
	}

	return nil
}

func (b *block) writeColumnsBuffer(columns ...column.ColumnCore) error {
	numRows := columns[0].NumRow()
	for i, column := range b.ColumnsHeader {
		if numRows != columns[i].NumRow() {
			return &NumberWriteError{
				FirstNumRow: numRows,
				NumRow:      columns[i].NumRow(),
				Column:      string(column.Name),
				FirstColumn: string(b.ColumnsHeader[0].Name),
			}
		}
		b.headerWriter.Reset()
		b.headerWriter.ByteString(column.Name)
		b.headerWriter.ByteString(column.ChType)

		if b.c.serverInfo.Revision >= helper.DbmsMinProtocolWithCustomSerialization {
			b.headerWriter.Uint8(0)
		}

		columns[i].HeaderWriter(b.headerWriter)
		if _, err := b.headerWriter.WriteTo(b.c.writerToCompress); err != nil {
			return &writeError{"block: write header block data for column " + string(column.Name), err}
		}
		if _, err := columns[i].WriteTo(b.c.writerToCompress); err != nil {
			return &writeError{"block: write block data for column " + string(column.Name), err}
		}
	}
	err := b.c.flushCompress()
	if err != nil {
		return &writeError{"block: flush block data", err}
	}
	return nil
}

func (b *block) getColumnsByChType() ([]column.ColumnCore, error) {
	columns := make([]column.ColumnCore, len(b.ColumnsHeader))
	for i, col := range b.ColumnsHeader {
		columnByType, err := column.ColumnByType(col.ChType, 0, false, false, b.c.serverInfo.Timezone)
		if err != nil {
			return nil, err
		}
		err = columnByType.SetColumnHeader(col)
		if err != nil {
			return nil, fmt.Errorf("set column header %q: %w", string(col.Name), err)
		}
		columns[i] = columnByType
	}
	return columns, nil
}

type blockInfo struct {
	field1      uint64
	isOverflows uint8
	field2      uint64
	bucketNum   int32
	num3        uint64
}

func (info *blockInfo) read(r *readerwriter.Reader) error {
	var err error
	if info.field1, err = r.Uvarint(); err != nil {
		return &readError{"blockInfo: read field1", err}
	}
	if info.isOverflows, err = r.ReadByte(); err != nil {
		return &readError{"blockInfo: read isOverflows", err}
	}
	if info.field2, err = r.Uvarint(); err != nil {
		return &readError{"blockInfo: read field2", err}
	}
	if info.bucketNum, err = r.Int32(); err != nil {
		return &readError{"blockInfo: read bucketNum", err}
	}
	if info.num3, err = r.Uvarint(); err != nil {
		return &readError{"blockInfo: read num3", err}
	}
	return nil
}

func (info *blockInfo) write(w *readerwriter.Writer) {
	w.Uvarint(1)
	w.Uint8(info.isOverflows)
	w.Uvarint(2)

	if info.bucketNum == 0 {
		info.bucketNum = -1
	}
	w.Int32(info.bucketNum)
	w.Uvarint(0)
}
