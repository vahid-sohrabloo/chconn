package column

import (
	"bytes"
	"fmt"
	"strconv"
)

type Enum16 struct {
	Int16
	oldChType      []byte
	intToStringMap map[int16]string
	stringToIntMap map[string]int16
}

// NewEnum16 return new Enum16 for Enum16 ClickHouse DataType
func NewEnum16(nullable bool) *Enum16 {
	return &Enum16{
		Int16: Int16{
			dict: make(map[int16]int),
			column: column{
				nullable:    nullable,
				colNullable: newNullable(),
				size:        Int16Size,
			},
		},
	}
}

// IntToStringMap return map for get string of enum by id
func (c *Enum16) IntToStringMap() (map[int16]string, error) {
	if c.intToStringMap == nil || !bytes.Equal(c.oldChType, c.chType) {
		if err := c.fillMaps(); err != nil {
			return nil, err
		}
		c.oldChType = c.chType
	}
	return c.intToStringMap, nil
}

// StringToIntMap return map for get id of enum by string
func (c *Enum16) StringToIntMap() (map[string]int16, error) {
	if c.intToStringMap == nil || !bytes.Equal(c.oldChType, c.chType) {
		if err := c.fillMaps(); err != nil {
			return nil, err
		}
		c.oldChType = c.chType
	}
	return c.stringToIntMap, nil
}

func (c *Enum16) fillMaps() error {
	chType := c.chType
	if bytes.HasPrefix(chType, []byte("Nullable(")) {
		chType = chType[len("Nullable(") : len(chType)-1]
	}
	enums := bytes.Split(chType[len("Enum16("):len(chType)-1], []byte(", "))
	c.intToStringMap = make(map[int16]string)
	c.stringToIntMap = make(map[string]int16)
	for _, enum := range enums {
		parts := bytes.SplitN(enum, []byte(" = "), 2)
		if len(parts) != 2 {
			//nolint:goerr113
			return fmt.Errorf("invalid enum: %s", enum)
		}

		id, err := strconv.ParseInt(string(parts[1]), 10, 8)
		if err != nil {
			//nolint:goerr113
			return fmt.Errorf("invalid enum id: %s", parts[1])
		}

		val := string(parts[0][1 : len(parts[0])-1])
		c.intToStringMap[int16(id)] = val
		c.stringToIntMap[val] = int16(id)
	}
	return nil
}
