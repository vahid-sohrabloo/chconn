package helper

type BinaryTypeIndex uint8

const (
	BinaryTypeIndexNothing                 BinaryTypeIndex = 0x00
	BinaryTypeIndexUInt8                   BinaryTypeIndex = 0x01
	BinaryTypeIndexUInt16                  BinaryTypeIndex = 0x02
	BinaryTypeIndexUInt32                  BinaryTypeIndex = 0x03
	BinaryTypeIndexUInt64                  BinaryTypeIndex = 0x04
	BinaryTypeIndexUInt128                 BinaryTypeIndex = 0x05
	BinaryTypeIndexUInt256                 BinaryTypeIndex = 0x06
	BinaryTypeIndexInt8                    BinaryTypeIndex = 0x07
	BinaryTypeIndexInt16                   BinaryTypeIndex = 0x08
	BinaryTypeIndexInt32                   BinaryTypeIndex = 0x09
	BinaryTypeIndexInt64                   BinaryTypeIndex = 0x0A
	BinaryTypeIndexInt128                  BinaryTypeIndex = 0x0B
	BinaryTypeIndexInt256                  BinaryTypeIndex = 0x0C
	BinaryTypeIndexFloat32                 BinaryTypeIndex = 0x0D
	BinaryTypeIndexFloat64                 BinaryTypeIndex = 0x0E
	BinaryTypeIndexDate                    BinaryTypeIndex = 0x0F
	BinaryTypeIndexDate32                  BinaryTypeIndex = 0x10
	BinaryTypeIndexDateTimeUTC             BinaryTypeIndex = 0x11
	BinaryTypeIndexDateTimeWithTimezone    BinaryTypeIndex = 0x12
	BinaryTypeIndexDateTime64UTC           BinaryTypeIndex = 0x13
	BinaryTypeIndexDateTime64WithTimezone  BinaryTypeIndex = 0x14
	BinaryTypeIndexString                  BinaryTypeIndex = 0x15
	BinaryTypeIndexFixedString             BinaryTypeIndex = 0x16
	BinaryTypeIndexEnum8                   BinaryTypeIndex = 0x17
	BinaryTypeIndexEnum16                  BinaryTypeIndex = 0x18
	BinaryTypeIndexDecimal32               BinaryTypeIndex = 0x19
	BinaryTypeIndexDecimal64               BinaryTypeIndex = 0x1A
	BinaryTypeIndexDecimal128              BinaryTypeIndex = 0x1B
	BinaryTypeIndexDecimal256              BinaryTypeIndex = 0x1C
	BinaryTypeIndexUUID                    BinaryTypeIndex = 0x1D
	BinaryTypeIndexArray                   BinaryTypeIndex = 0x1E
	BinaryTypeIndexUnnamedTuple            BinaryTypeIndex = 0x1F
	BinaryTypeIndexNamedTuple              BinaryTypeIndex = 0x20
	BinaryTypeIndexSet                     BinaryTypeIndex = 0x21
	BinaryTypeIndexInterval                BinaryTypeIndex = 0x22
	BinaryTypeIndexNullable                BinaryTypeIndex = 0x23
	BinaryTypeIndexFunction                BinaryTypeIndex = 0x24
	BinaryTypeIndexAggregateFunction       BinaryTypeIndex = 0x25
	BinaryTypeIndexLowCardinality          BinaryTypeIndex = 0x26
	BinaryTypeIndexMap                     BinaryTypeIndex = 0x27
	BinaryTypeIndexIPv4                    BinaryTypeIndex = 0x28
	BinaryTypeIndexIPv6                    BinaryTypeIndex = 0x29
	BinaryTypeIndexVariant                 BinaryTypeIndex = 0x2A
	BinaryTypeIndexDynamic                 BinaryTypeIndex = 0x2B
	BinaryTypeIndexCustom                  BinaryTypeIndex = 0x2C
	BinaryTypeIndexBool                    BinaryTypeIndex = 0x2D
	BinaryTypeIndexSimpleAggregateFunction BinaryTypeIndex = 0x2E
	BinaryTypeIndexNested                  BinaryTypeIndex = 0x2F
	BinaryTypeIndexJSON                    BinaryTypeIndex = 0x30
	BinaryTypeIndexBFloat16                BinaryTypeIndex = 0x31
	BinaryTypeIndexTime                    BinaryTypeIndex = 0x32
	BinaryTypeIndexTime64                  BinaryTypeIndex = 0x34
)
