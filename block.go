package chconn

import (
	"fmt"
	"strings"
)

type Column struct {
	ChType      string
	Name        string
	BufferIndex int
	NumBuffer   int
}

type Block struct {
	Columns       []*Column
	ColumnsBuffer []*Writer
	NumRows       uint64
	readRows      uint64
	NumColumns    uint64
	info          blockInfo
}

func NewBlock() *Block {
	return &Block{}
}

func (block *Block) Read(ch *Conn) (err error) {
	if err = block.info.read(ch.reader); err != nil {
		return err
	}

	if block.NumColumns, err = ch.reader.Uvarint(); err != nil {
		return err
	}

	if block.NumRows, err = ch.reader.Uvarint(); err != nil {
		return err
	}
	return nil
}

func (block *Block) initForInsert(ch *Conn) error {

	if block.NumRows > 0 || block.NumColumns == 0 {
		return ErrNotInsertQuery
	}

	block.Columns = make([]*Column, block.NumColumns)

	for i := uint64(0); i < block.NumColumns; i++ {

		column, err := block.NextColumn(ch)
		if err != nil {
			return err
		}
		column.BufferIndex = len(block.ColumnsBuffer)
		block.appendBuffer(column.ChType, column)
		block.Columns[i] = column
	}
	return nil
}

func (block *Block) readColumns(ch *Conn) error {
	block.Columns = make([]*Column, block.NumColumns)

	for i := uint64(0); i < block.NumColumns; i++ {

		column, err := block.NextColumn(ch)
		if err != nil {
			return err
		}
		block.Columns[i] = column
	}
	return nil
}

func (block *Block) NextColumn(ch *Conn) (*Column, error) {
	column := &Column{}
	var err error
	if column.Name, err = ch.reader.String(); err != nil {
		return nil, fmt.Errorf("block: read column name: %w", err)
	}
	if column.ChType, err = ch.reader.String(); err != nil {
		return column, fmt.Errorf("block: read column type: %w", err)
	}
	return column, nil
}
func (block *Block) appendBuffer(chType string, column *Column) {
	if strings.HasPrefix(chType, "SimpleAggregateFunction") || strings.HasPrefix(chType, "AggregateFunction") {
		return
	}
	block.ColumnsBuffer = append(block.ColumnsBuffer, NewWriter())
	column.NumBuffer++
	var findSimpleAggregateFunction bool
	for i, char := range chType {
		if char == ',' || (char == '(' && chType[i-1] != 'g' && chType[i-1] != '2' && chType[i-1] != '4') {
			//skip tuple
			if i-5 >= 0 && chType[i-5:i] == "Tuple" {
				continue
			}

			// skip SimpleAggregateFunction
			if i-23 >= 0 && chType[i-23:i] == "SimpleAggregateFunction" {
				findSimpleAggregateFunction = true
				continue
			}
			if findSimpleAggregateFunction {
				findSimpleAggregateFunction = false
				continue
			}
			block.ColumnsBuffer = append(block.ColumnsBuffer, NewWriter())
			column.NumBuffer++
		}
	}
}

func commaOrParentheses(c rune) bool {
	return c == ',' || c == '('
}

func (block *Block) write(ch *Conn) error {
	if err := block.info.write(ch.writer); err != nil {
		return err
	}
	ch.writer.Uvarint(block.NumColumns)
	ch.writer.Uvarint(block.NumRows)
	defer func() {
		block.NumRows = 0
	}()
	var bufferIndex int
	for _, column := range block.Columns {
		ch.writer.String(column.Name)
		ch.writer.String(column.ChType)
		for i := 0; i < column.NumBuffer; i++ {
			if _, err := block.ColumnsBuffer[bufferIndex].WriteTo(ch.writer.output); err != nil {
				return err
			}
			bufferIndex++
		}
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

func (info *blockInfo) read(r *Reader) error {
	var err error
	if info.field1, err = r.Uvarint(); err != nil {
		return err
	}
	if info.isOverflows, err = r.Bool(); err != nil {
		return err
	}
	if info.field2, err = r.Uvarint(); err != nil {
		return err
	}
	if info.bucketNum, err = r.Int32(); err != nil {
		return err
	}
	if info.num3, err = r.Uvarint(); err != nil {
		return err
	}
	return nil
}

func (info *blockInfo) write(w *Writer) error {
	if err := w.Uvarint(1); err != nil {
		return err
	}
	if err := w.Bool(info.isOverflows); err != nil {
		return err
	}
	if err := w.Uvarint(2); err != nil {
		return err
	}

	if info.bucketNum == 0 {
		info.bucketNum = -1
	}
	if err := w.Int32(info.bucketNum); err != nil {
		return err
	}
	if err := w.Uvarint(0); err != nil {
		return err
	}
	return nil
}
