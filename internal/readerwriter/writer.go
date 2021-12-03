package readerwriter

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"reflect"
	"unsafe"
)

const (
	// Need to read additional keys.
	// Additional keys are stored before indexes as value N and N keys
	// after them.
	hasAdditionalKeysBit = 1 << 9
	// Need to update dictionary.
	// It means that previous granule has different dictionary.
	needUpdateDictionary = 1 << 10

	serializationType = hasAdditionalKeysBit | needUpdateDictionary
)

// Writer is a helper to write data into bytes.Buffer
type Writer struct {
	output                  *bytes.Buffer
	offset                  uint64
	isLowCardinality        bool
	isLCNull                bool
	keyLC                   []int
	keyStringDictionaryLC   map[string]int
	stringDictionaryLC      []string
	fixedStringDictionaryLC [][]byte
	scratch                 [binary.MaxVarintLen64]byte
}

// NewWriter get new writer
func NewWriter() *Writer {
	return &Writer{
		output: &bytes.Buffer{},
	}
}

// Uvarint write a variable uint64 value into writer
func (w *Writer) Uvarint(v uint64) {
	ln := binary.PutUvarint(w.scratch[:binary.MaxVarintLen64], v)
	w.Write(w.scratch[:ln])
}

// Bool write bool value
func (w *Writer) Bool(v bool) {
	if v {
		w.Uint8(1)
		return
	}
	w.Uint8(0)
}

// Int8 write Int8 value
func (w *Writer) Int8(v int8) {
	w.Uint8(uint8(v))
}

// Int16 write Int16 value
func (w *Writer) Int16(v int16) {
	w.Uint16(uint16(v))
}

// Int32 write Int32 value
func (w *Writer) Int32(v int32) {
	w.Uint32(uint32(v))
}

// Int64 write Int64 value
func (w *Writer) Int64(v int64) {
	w.Uint64(uint64(v))
}

// Uint8 write Uint8 value
func (w *Writer) Uint8(v uint8) {
	w.output.WriteByte(v)
}

// Uint16 write Uint16 value
func (w *Writer) Uint16(v uint16) {
	w.scratch[0] = byte(v)
	w.scratch[1] = byte(v >> 8)
	w.Write(w.scratch[:2])
}

// Uint32 write Uint32 value
func (w *Writer) Uint32(v uint32) {
	w.scratch[0] = byte(v)
	w.scratch[1] = byte(v >> 8)
	w.scratch[2] = byte(v >> 16)
	w.scratch[3] = byte(v >> 24)
	w.Write(w.scratch[:4])
}

// Uint64 write Uint64 value
func (w *Writer) Uint64(v uint64) {
	w.scratch[0] = byte(v)
	w.scratch[1] = byte(v >> 8)
	w.scratch[2] = byte(v >> 16)
	w.scratch[3] = byte(v >> 24)
	w.scratch[4] = byte(v >> 32)
	w.scratch[5] = byte(v >> 40)
	w.scratch[6] = byte(v >> 48)
	w.scratch[7] = byte(v >> 56)
	w.Write(w.scratch[:8])
}

// Float32 write Float32 value
func (w *Writer) Float32(v float32) {
	w.Uint32(math.Float32bits(v))
}

// Float64 write Float64 value
func (w *Writer) Float64(v float64) {
	w.Uint64(math.Float64bits(v))
}

// AddLen add len of array
func (w *Writer) AddLen(v uint64) {
	w.offset += v
	w.Uint64(w.offset)
}

// SetStringLowCardinalityNull set LowCardinality is nullable
func (w *Writer) SetStringLowCardinalityNull() {
	w.isLCNull = true
}

// AddStringLowCardinality add string to LowCardinality dictionary
func (w *Writer) AddStringLowCardinality(v string) {
	w.isLowCardinality = true
	if w.keyStringDictionaryLC == nil {
		w.keyStringDictionaryLC = make(map[string]int)
	}

	key, ok := w.keyStringDictionaryLC[v]
	if !ok {
		key = len(w.keyStringDictionaryLC)
		w.keyStringDictionaryLC[v] = key
		w.stringDictionaryLC = append(w.stringDictionaryLC, v)
	}
	w.keyLC = append(w.keyLC, key)
}

// AddStringLowCardinality add fixed string to LowCardinality dictionary
func (w *Writer) AddFixedStringLowCardinality(v []byte) {
	w.isLowCardinality = true
	if w.keyStringDictionaryLC == nil {
		w.keyStringDictionaryLC = make(map[string]int)
	}

	key, ok := w.keyStringDictionaryLC[string(v)]
	if !ok {
		key = len(w.keyStringDictionaryLC)
		w.keyStringDictionaryLC[string(v)] = key
		w.fixedStringDictionaryLC = append(w.fixedStringDictionaryLC, v)
	}
	w.keyLC = append(w.keyLC, key)
}

// flushLowCardinality flush LowCardinality dictionary to buffer
func (w *Writer) flushLowCardinality() {
	var intType int
	var dictionarySize int
	if w.isLCNull {
		dictionarySize++
	}
	if len(w.stringDictionaryLC) > 0 {
		dictionarySize += len(w.stringDictionaryLC)
		intType = int(math.Log2(float64(dictionarySize)) / 8)
		stype := serializationType | intType
		w.Int64(int64(stype))
		w.Int64(int64(dictionarySize))

		// write null value
		if w.isLCNull {
			w.String("")
		}

		for _, val := range w.stringDictionaryLC {
			w.String(val)
		}
	} else {
		dictionarySize += len(w.fixedStringDictionaryLC)
		intType = int(math.Log2(float64(dictionarySize)) / 8)
		stype := serializationType | intType
		w.Int64(int64(stype))
		w.Int64(int64(dictionarySize))

		// write null value
		if w.isLCNull {
			w.Write([]byte{})
		}
		for _, val := range w.fixedStringDictionaryLC {
			w.Write(val)
		}
	}

	var keysOffset int
	if w.isLCNull {
		keysOffset = 1
	}
	w.Int64(int64(len(w.keyLC)))
	switch intType {
	case 0:
		for _, val := range w.keyLC {
			w.Uint8(uint8(val + keysOffset))
		}
	case 1:
		for _, val := range w.keyLC {
			w.Uint16(uint16(val + keysOffset))
		}
	case 2:
		for _, val := range w.keyLC {
			w.Uint32(uint32(val + keysOffset))
		}
	case 3:
		for _, val := range w.keyLC {
			w.Uint64(uint64(val + keysOffset))
		}
	}
}

// String write string
func (w *Writer) String(v string) {
	str := str2Bytes(v)
	w.Uvarint(uint64(len(str)))
	w.Write(str)
}

// Buffer write []byte
func (w *Writer) Buffer(str []byte) {
	w.Uvarint(uint64(len(str)))
	w.Write(str)
}

// Write write raw []byte data
func (w *Writer) Write(b []byte) {
	w.output.Write(b)
}

// ReadFrom reads data from r until EOF and appends it to the buffer
// NOTE: after use this function you can't add any LowCardinality data
func (w *Writer) ReadFrom(r io.Reader) (n int64, err error) {
	return w.output.ReadFrom(r)
}

// WriteTo implement WriteTo
func (w *Writer) WriteTo(wt io.Writer) (int64, error) {
	return w.output.WriteTo(wt)
}

// Bytes get current bytes
// NOTE: after use this function you can't add any LowCardinality data
func (w *Writer) Bytes() []byte {
	if w.isLowCardinality {
		w.flushLowCardinality()
		w.isLowCardinality = false
	}
	return w.output.Bytes()
}

// Reset reset all data
func (w *Writer) Reset() {
	w.offset = 0
	if w.stringDictionaryLC != nil {
		w.stringDictionaryLC = w.stringDictionaryLC[:0]
	}
	if w.fixedStringDictionaryLC != nil {
		w.fixedStringDictionaryLC = w.fixedStringDictionaryLC[:0]
	}
	if w.keyStringDictionaryLC != nil {
		w.keyStringDictionaryLC = nil
	}

	w.keyLC = w.keyLC[:0]
	w.output.Reset()
}

// Outout get raw *bytes.Buffer
func (w *Writer) Output() *bytes.Buffer {
	return w.output
}

func str2Bytes(str string) []byte {
	header := (*reflect.SliceHeader)(unsafe.Pointer(&str))
	header.Len = len(str)
	header.Cap = header.Len
	return *(*[]byte)(unsafe.Pointer(header))
}
