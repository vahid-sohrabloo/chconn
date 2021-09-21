package main

import (
	"fmt"
	"os"
)

type columns struct {
	ChType         string
	GoType         string
	ExtraArgsInput string
	ExtraArgsCall  string
}

//nolint:funlen,gocyclo
func main() {
	if len(os.Args) <= 1 {
		fmt.Println("please provice cmd")
		return
	}
	cmd := os.Args[1]
	types := []columns{
		{"Int8", "int8", "", ""},
		{"Int16", "int16", "", ""},
		{"Int32", "int32", "", ""},
		{"Decimal32", "float64", ", scale int", "scale"},
		{"Decimal64", "float64", ", scale int", "scale"},
		{"Int64", "int64", "", ""},
		{"Uint8", "uint8", "", ""},
		{"Uint16", "uint16", "", ""},
		{"Uint32", "uint32", "", ""},
		{"Uint64", "uint64", "", ""},
		{"Float32", "float32", "", ""},
		{"Float64", "float64", "", ""},
		{"String", "string", "", ""},
		{"ByteArray", "[]byte", "", ""},
		{"FixedString", "[]byte", ", strlen int", "strlen"},
		{"Date", "time.Time", "", ""},
		{"DateTime", "time.Time", "", ""},
		{"DateTime64", "time.Time", ", precision int", "precision"},
		{"UUID", "[16]byte", "", ""},
		{"IPv4", "net.IP", "", ""},
		{"IPv6", "net.IP", "", ""},
	}
	fmt.Println(`package chconn
import (
	"net"
	"time"
)
	`)

	if cmd == "slice" {
		for _, t := range types {
			fmt.Printf(`// %[1]sS read num of %[1]s values
func (s *selectStmt) %[1]sS(num uint64, value *[]%[2]s%[3]s) error {
	var (
		val %[2]s
		err error
	)
	for i := uint64(0); i < num; i++ {
		val, err = s.%[1]s(%[4]s)
		if err != nil {
			return err
		}
		*value = append(*value, val)
	}
	return nil
}
	
	`, t.ChType, t.GoType, t.ExtraArgsInput, t.ExtraArgsCall)
		}

		return
	}
	if cmd == "slice_all" {
		for _, t := range types {
			extraArgsCall := t.ExtraArgsCall
			if extraArgsCall != "" {
				extraArgsCall = ", " + extraArgsCall
			}
			fmt.Printf(`// %[1]sAll reads all %[1]s values from a block
func (s *selectStmt) %[1]sAll(value *[]%[2]s%[3]s) error {
	return s.%[1]sS(s.block.NumRows,value%[4]s)
}

`, t.ChType, t.GoType, t.ExtraArgsInput, extraArgsCall)
		}
	}

	if cmd == "slice_null" {
		for _, t := range types {
			gotype := t.GoType
			if gotype != "[]byte" {
				gotype = "*" + gotype
			}
			extraArgsCall := t.ExtraArgsCall
			if extraArgsCall != "" {
				extraArgsCall = ", " + extraArgsCall
			}
			fmt.Printf(`// %[1]sPS read num of %[1]s null values from a block
// NOTE: Should read nulls with GetNullS or GetNullAll and pass it to this function
func (s *selectStmt) %[1]sPS(num uint64, nulls []uint8, values *[]%[2]s%[3]s) error {
	return s.%[1]sPCallback(num, nulls, func(val %[2]s) {
		*values = append(*values, val)
	}%[4]s)
}

`, t.ChType, gotype, t.ExtraArgsInput, extraArgsCall)
		}
	}

	if cmd == "slice_null_all" {
		for _, t := range types {
			gotype := t.GoType
			if gotype != "[]byte" {
				gotype = "*" + gotype
			}
			extraArgsCall := t.ExtraArgsCall
			if extraArgsCall != "" {
				extraArgsCall = ", " + extraArgsCall
			}
			fmt.Printf(`
// %[1]sPAll read all %[1]s null values from a block
func (s *selectStmt) %[1]sPAll(values *[]%[2]s%[3]s) error {
	nulls, err := s.GetNullS(s.block.NumRows)
	if err != nil {
		return &readError{"selectStmt: read nulls", err}
	}
	return s.%[1]sPS(s.block.NumRows, nulls, values%[4]s)
}

`, t.ChType, gotype, t.ExtraArgsInput, extraArgsCall)
		}
	}

	if cmd == "slice_null_callback" {
		for _, t := range types {
			gotype := t.GoType
			if gotype != "[]byte" {
				gotype = "*" + gotype
			}
			extraArgsCall := t.ExtraArgsCall
			fmt.Printf(`
// %[1]sPCallback read num of %[1]s null values from a block and send it to callback
// NOTE: Should read nulls with GetNullS or GetNullAll and pass it to this function
func (s *selectStmt) %[1]sPCallback(num uint64, nulls []uint8, cb func(%[2]s)%[3]s) error {
	for i := uint64(0); i < num; i++ {
		val, err := s.%[1]s(%[4]s)
		if err != nil {
			return err
		}
		if nulls[i] == 0 {
			cb(&val)
		} else {
			cb(nil)
		}
	}
	return nil
}

`, t.ChType, gotype, t.ExtraArgsInput, extraArgsCall)
		}
	}
}
