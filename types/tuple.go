package types

import "github.com/vahid-sohrabloo/chconn/v2/column"

var _ = &column.TupleOf[Point]{}

type Point struct {
	X float64
	Y float64
}

func (t Point) Append(columns []column.ColumnBasic) {
	columns[0].(*column.Base[float64]).Append(t.X)
	columns[1].(*column.Base[float64]).Append(t.Y)
}

func (t Point) Get(columns []column.ColumnBasic, row int) Point {
	return Point{
		X: columns[0].(*column.Base[float64]).Row(row),
		Y: columns[1].(*column.Base[float64]).Row(row),
	}
}

func (t Point) Column() []column.ColumnBasic {
	return []column.ColumnBasic{
		column.New[float64](),
		column.New[float64](),
	}
}
