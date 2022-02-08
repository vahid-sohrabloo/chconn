//nolint:dupl
package column

import (
	"bytes"
	"fmt"
	"strconv"
)

//  Enum8 use for Enum8 ClickHouse DataType
type Enum8 struct {
	Int8
	oldChType      []byte
	intToStringMap map[int8]string
	stringToIntMap map[string]int8
}

// NewEnum8 return new Enum8 for Enum8 ClickHouse DataType
func NewEnum8(nullable bool) *Enum8 {
	return &Enum8{
		Int8: Int8{
			dict: make(map[int8]int),
			column: column{
				nullable:    nullable,
				colNullable: newNullable(),
				size:        Int8Size,
			},
		},
	}
}

// IntToStringMap return map for get string of enum by id
func (c *Enum8) IntToStringMap() (map[int8]string, error) {
	if c.intToStringMap == nil || !bytes.Equal(c.oldChType, c.chType) {
		if err := c.fillMaps(); err != nil {
			return nil, err
		}
		c.oldChType = c.chType
	}
	return c.intToStringMap, nil
}

// StringToIntMap return map for get id of enum by string
func (c *Enum8) StringToIntMap() (map[string]int8, error) {
	if c.stringToIntMap == nil || !bytes.Equal(c.oldChType, c.chType) {
		if err := c.fillMaps(); err != nil {
			return nil, err
		}
		c.oldChType = c.chType
	}
	return c.stringToIntMap, nil
}

func (c *Enum8) fillMaps() error {
	chType := c.chType
	if bytes.HasPrefix(chType, []byte("Nullable(")) {
		chType = chType[len("Nullable(") : len(chType)-1]
	}
	enums := bytes.Split(chType[len("Enum8("):len(chType)-1], []byte(", "))
	c.intToStringMap = make(map[int8]string)
	c.stringToIntMap = make(map[string]int8)
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
		c.intToStringMap[int8(id)] = val
		c.stringToIntMap[val] = int8(id)
	}
	return nil
}
