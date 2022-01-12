package chconn

import (
	"github.com/vahid-sohrabloo/chconn/column"
	"github.com/vahid-sohrabloo/chconn/internal/readerwriter"
)

// Column contains details of ClickHouse column with Buffer index in inserting
type Column struct {
	ChType string
	Name   string
}

type block struct {
	Columns      []*Column
	NumRows      uint64
	NumColumns   uint64
	info         blockInfo
	headerWriter *readerwriter.Writer
}

func newBlock() *block {
	return &block{
		headerWriter: readerwriter.NewWriter(),
	}
}

func (block *block) read(ch *conn) error {
	if _, err := ch.reader.String(); err != nil { // temporary table
		return err
	}

	ch.reader.SetCompress(ch.compress)
	defer ch.reader.SetCompress(false)
	var err error
	err = block.info.read(ch.reader)
	if err != nil {
		return err
	}

	block.NumColumns, err = ch.reader.Uvarint()
	if err != nil {
		return &readError{"block: read NumColumns", err}
	}

	block.NumRows, err = ch.reader.Uvarint()
	if err != nil {
		return &readError{"block: read NumRows", err}
	}
	return nil
}

func (block *block) initForInsert(ch *conn) error {
	ch.reader.SetCompress(ch.compress)
	defer ch.reader.SetCompress(false)
	block.Columns = make([]*Column, block.NumColumns)
	for i := uint64(0); i < block.NumColumns; i++ {
		col, err := block.nextColumn(ch)
		if err != nil {
			return err
		}
		block.Columns[i] = col
	}

	return nil
}

func (block *block) readColumns(ch *conn) error {
	block.Columns = make([]*Column, block.NumColumns)

	for i := uint64(0); i < block.NumColumns; i++ {
		col, err := block.nextColumn(ch)
		if err != nil {
			return err
		}
		block.Columns[i] = col
	}
	return nil
}

func (block *block) nextColumn(ch *conn) (*Column, error) {
	col := &Column{}
	var err error
	if col.Name, err = ch.reader.String(); err != nil {
		return nil, &readError{"block: read column name", err}
	}
	if col.ChType, err = ch.reader.String(); err != nil {
		return col, &readError{"block: read column type", err}
	}
	return col, nil
}

func (block *block) writeHeader(ch *conn, numRows int) error {
	block.info.write(ch.writer)
	// NumColumns
	ch.writer.Uvarint(block.NumColumns)
	// NumRows
	ch.writer.Uvarint(uint64(numRows))
	_, err := ch.writer.WriteTo(ch.writerToCompress)
	if err != nil {
		return &writeError{"block: write block info", err}
	}
	err = ch.flushCompress()
	if err != nil {
		return &writeError{"block: flush block info", err}
	}

	return nil
}

func (block *block) writeColumnsBuffer(ch *conn, columns ...column.Column) error {
	if int(block.NumColumns) != len(columns) {
		return &ColumnNumberWriteError{
			WriteColumn: len(columns),
			NeedColumn:  block.NumColumns,
		}
	}
	numRows := columns[0].NumRow()
	for i, column := range block.Columns {
		if numRows != columns[i].NumRow() {
			return &NumberWriteError{
				FirstNumRow: numRows,
				NumRow:      columns[i].NumRow(),
				Column:      column.Name,
			}
		}
		block.headerWriter.Reset()
		block.headerWriter.String(column.Name)
		block.headerWriter.String(column.ChType)

		columns[i].HeaderWriter(block.headerWriter)
		if _, err := block.headerWriter.WriteTo(ch.writerToCompress); err != nil {
			return &writeError{"block: write header block data for column " + column.Name, err}
		}
		if _, err := columns[i].WriteTo(ch.writerToCompress); err != nil {
			return &writeError{"block: write block data for column " + column.Name, err}
		}
	}
	err := ch.flushCompress()
	if err != nil {
		return &writeError{"block: flush block data", err}
	}
	return nil
}

type blockInfo struct {
	field1      uint64
	isOverflows bool
	field2      uint64
	bucketNum   int32
	num3        uint64
}

func (info *blockInfo) read(r *readerwriter.Reader) error {
	var err error
	if info.field1, err = r.Uvarint(); err != nil {
		return &readError{"blockInfo: read field1", err}
	}
	if info.isOverflows, err = r.Bool(); err != nil {
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
	w.Bool(info.isOverflows)
	w.Uvarint(2)

	if info.bucketNum == 0 {
		info.bucketNum = -1
	}
	w.Int32(info.bucketNum)
	w.Uvarint(0)
}
