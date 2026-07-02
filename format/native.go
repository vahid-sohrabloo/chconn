package format

import (
	"fmt"
	"io"

	"github.com/vahid-sohrabloo/chconn/v3"
	"github.com/vahid-sohrabloo/chconn/v3/column"
	"github.com/vahid-sohrabloo/chconn/v3/internal/readerwriter"
	"github.com/vahid-sohrabloo/chconn/v3/shared"
)

const maxColumns = 1 << 20

// NativeWriter writes columns in ClickHouse Native binary format to an io.Writer.
type NativeWriter struct {
	w            io.Writer
	headerWriter *readerwriter.Writer
	columns      []column.ColumnCore
}

// NewNativeWriter creates a new NativeWriter that writes to w.
func NewNativeWriter(w io.Writer) *NativeWriter {
	return &NativeWriter{
		w:            w,
		headerWriter: readerwriter.NewWriter(),
	}
}

// WriteBlock writes pre-populated columns as one Native block.
// All columns must have the same number of rows.
func (n *NativeWriter) WriteBlock(columns ...column.ColumnCore) error {
	if len(columns) == 0 {
		return nil
	}

	numRows := columns[0].NumRow()
	for i := 1; i < len(columns); i++ {
		if columns[i].NumRow() != numRows {
			return fmt.Errorf("native: column %q has %d rows, expected %d",
				string(columns[i].Name()), columns[i].NumRow(), numRows)
		}
	}

	// Write block header: NumColumns, NumRows
	n.headerWriter.Reset()
	n.headerWriter.Uvarint(uint64(len(columns)))
	n.headerWriter.Uvarint(uint64(numRows))
	if _, err := n.headerWriter.WriteTo(n.w); err != nil {
		return fmt.Errorf("native: write block header: %w", err)
	}

	// Write each column
	for _, col := range columns {
		n.headerWriter.Reset()
		n.headerWriter.ByteString(col.Name())
		n.headerWriter.ByteString(col.Type())
		col.HeaderWriter(n.headerWriter)
		if _, err := n.headerWriter.WriteTo(n.w); err != nil {
			return fmt.Errorf("native: write column %q header: %w", string(col.Name()), err)
		}
		if _, err := col.WriteTo(n.w); err != nil {
			return fmt.Errorf("native: write column %q data: %w", string(col.Name()), err)
		}
	}

	return nil
}

// SetColumns defines the schema for Append mode.
// Column objects are auto-created via column.ColumnByType.
func (n *NativeWriter) SetColumns(headers []column.ColumnHeader) error {
	columns := make([]column.ColumnCore, len(headers))
	for i, h := range headers {
		col, err := column.ColumnByType(h.ChType, 0, false, false, "")
		if err != nil {
			return fmt.Errorf("native: create column %q (type %s): %w", string(h.Name), string(h.ChType), err)
		}
		if err := col.SetColumnHeader(h); err != nil {
			return fmt.Errorf("native: set column header %q: %w", string(h.Name), err)
		}
		columns[i] = col
	}
	n.columns = columns
	return nil
}

// Append adds one row of values. Requires SetColumns to be called first.
func (n *NativeWriter) Append(values ...any) error {
	if n.columns == nil {
		return fmt.Errorf("native: SetColumns must be called before Append")
	}
	if len(values) != len(n.columns) {
		return fmt.Errorf("native: expected %d values, got %d", len(n.columns), len(values))
	}
	for i, value := range values {
		if err := n.columns[i].AppendAny(value); err != nil {
			return fmt.Errorf("native: append to column %q: %w", string(n.columns[i].Name()), err)
		}
	}
	return nil
}

// Flush writes accumulated rows as a block and resets columns.
func (n *NativeWriter) Flush() error {
	if n.columns == nil {
		return fmt.Errorf("native: SetColumns must be called before Flush")
	}
	if err := n.WriteBlock(n.columns...); err != nil {
		return err
	}
	for _, col := range n.columns {
		col.Reset()
	}
	return nil
}

// Columns returns the current column objects for direct typed access.
func (n *NativeWriter) Columns() []column.ColumnCore {
	return n.columns
}

// NativeReader reads ClickHouse Native binary format from an io.Reader.
type NativeReader struct {
	headerWriter *readerwriter.Writer
}

// NewNativeReader creates a new NativeReader.
func NewNativeReader() *NativeReader {
	return &NativeReader{
		headerWriter: readerwriter.NewWriter(),
	}
}

// ReadBlock reads one Native block from r, returning populated columns.
// Returns io.EOF when no more data is available.
func (n *NativeReader) ReadBlock(r io.Reader, serverInfo *shared.ServerInfo) ([]column.ColumnCore, error) {
	if serverInfo == nil {
		serverInfo = shared.EmptyServerInfo()
	}

	reader := readerwriter.NewReader(r)

	numColumns, err := reader.Uvarint()
	if err != nil {
		// If the reader is at EOF, return io.EOF
		return nil, fmt.Errorf("native: read num columns: %w", err)
	}

	if numColumns > maxColumns {
		return nil, fmt.Errorf("native: implausible column count %d", numColumns)
	}

	numRows, err := reader.Uvarint()
	if err != nil {
		return nil, fmt.Errorf("native: read num rows: %w", err)
	}

	columns := make([]column.ColumnCore, numColumns)
	for i := range numColumns {
		name, err := reader.ByteString()
		if err != nil {
			return nil, fmt.Errorf("native: read column name: %w", err)
		}

		chType, err := reader.ByteString()
		if err != nil {
			return nil, fmt.Errorf("native: read column type: %w", err)
		}

		col, err := column.ColumnByType(chType, 0, false, false, serverInfo.Timezone)
		if err != nil {
			return nil, fmt.Errorf("native: create column %q (type %s): %w", string(name), string(chType), err)
		}

		if err := col.SetColumnHeader(column.ColumnHeader{
			Name:   name,
			ChType: chType,
		}); err != nil {
			return nil, fmt.Errorf("native: set column header %q: %w", string(name), err)
		}

		if err := col.ReadHeader(reader, serverInfo); err != nil {
			return nil, fmt.Errorf("native: read header for column %q: %w", string(name), err)
		}

		if numRows > 0 {
			if err := col.ReadRaw(int(numRows)); err != nil {
				return nil, fmt.Errorf("native: read data for column %q: %w", string(name), err)
			}
		}

		columns[i] = col
	}

	return columns, nil
}

// ReadBlockInto reads one block into pre-existing columns.
// The column names and types from the stream must match the provided columns.
func (n *NativeReader) ReadBlockInto(r io.Reader, serverInfo *shared.ServerInfo, columns ...column.ColumnCore) error {
	if serverInfo == nil {
		serverInfo = shared.EmptyServerInfo()
	}

	reader := readerwriter.NewReader(r)

	numColumns, err := reader.Uvarint()
	if err != nil {
		return fmt.Errorf("native: read num columns: %w", err)
	}

	if numColumns > maxColumns {
		return fmt.Errorf("native: implausible column count %d", numColumns)
	}

	if int(numColumns) != len(columns) {
		return fmt.Errorf("native: expected %d columns, got %d in stream", len(columns), numColumns)
	}

	numRows, err := reader.Uvarint()
	if err != nil {
		return fmt.Errorf("native: read num rows: %w", err)
	}

	for i, col := range columns {
		name, err := reader.ByteString()
		if err != nil {
			return fmt.Errorf("native: read column %d name: %w", i, err)
		}

		chType, err := reader.ByteString()
		if err != nil {
			return fmt.Errorf("native: read column %d type: %w", i, err)
		}

		if err := col.SetColumnHeader(column.ColumnHeader{
			Name:   name,
			ChType: chType,
		}); err != nil {
			return fmt.Errorf("native: set column header %q: %w", string(name), err)
		}

		if err := col.ReadHeader(reader, serverInfo); err != nil {
			return fmt.Errorf("native: read header for column %q: %w", string(name), err)
		}

		if numRows > 0 {
			if err := col.ReadRaw(int(numRows)); err != nil {
				return fmt.Errorf("native: read data for column %q: %w", string(name), err)
			}
		}
	}

	return nil
}

// Read streams all blocks from a SelectStmt to w in Native binary format.
// This is the export path: SELECT → Native binary file.
func (n *NativeReader) Read(stmt chconn.SelectStmt, w io.Writer) error {
	writer := NewNativeWriter(w)
	for stmt.Next() {
		columns := stmt.Columns()
		if err := writer.WriteBlock(columns...); err != nil {
			return err
		}
	}
	return stmt.Err()
}
