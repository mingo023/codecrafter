package decoder

import (
	"encoding/binary"

	"github.com/google/uuid"
)

type BinaryDecoder struct {
	raw    []byte
	offset int
}

func (d *BinaryDecoder) Init(raw []byte) {
	d.raw = raw
	d.offset = 0
}

func (d *BinaryDecoder) GetInt8() int8 {
	val := int8(d.raw[d.offset])
	d.offset++
	return val
}

func (d *BinaryDecoder) GetInt16() int16 {
	val := int16(
		binary.BigEndian.Uint16(d.raw[d.offset:]),
	)
	d.offset += 2
	return val
}

func (d *BinaryDecoder) GetInt32() int32 {
	val := int32(
		binary.BigEndian.Uint32(d.raw[d.offset:]),
	)
	d.offset += 4
	return val
}

func (d *BinaryDecoder) GetInt64() int64 {
	val := int64(
		binary.BigEndian.Uint64(d.raw[d.offset:]),
	)
	d.offset += 8
	return val
}

func (d *BinaryDecoder) GetStringLen() int16 {
	val := int16(binary.BigEndian.Uint16(d.raw[d.offset:]))
	d.offset += 2

	return val
}

func (d *BinaryDecoder) GetString() string {
	len := d.GetStringLen()
	val := string(d.raw[d.offset : d.offset+int(len)])
	d.offset += int(len)

	return val
}

func (d *BinaryDecoder) GetEmptyTaggedFieldArray() any {
	tagsLength := d.GetInt8()
	if tagsLength != 0 {
		panic("Tagged fields are not supported in this version of the API")
	}

	return nil
}

func (d *BinaryDecoder) GetUnsignedVarint() uint64 {
	val, n := binary.Uvarint(d.raw[d.offset:])
	d.offset += n

	return val
}

func (d *BinaryDecoder) GetCompactArrayLen() int {
	val := d.GetUnsignedVarint()
	return int(val) - 1
}

func (d *BinaryDecoder) GetCompactString() string {
	length := d.GetUnsignedVarint() - 1
	val := string(d.raw[d.offset : d.offset+int(length)])
	d.offset += int(length)

	return val
}

func (d *BinaryDecoder) GetSignedVarint() int64 {
	val, n := binary.Varint(d.raw[d.offset:])
	d.offset += n

	return val
}

func (d *BinaryDecoder) GetUUID() uuid.UUID {
	val, err := uuid.FromBytes(d.raw[d.offset : d.offset+16])
	if err != nil {
		panic(err)
	}

	d.offset += 16

	return val
}

func (d *BinaryDecoder) GetCompactInt32Array() []int32 {
	length := d.GetCompactArrayLen()
	val := make([]int32, length)

	for i := 0; i < length; i++ {
		val[i] = d.GetInt32()
	}

	return val
}

func (d *BinaryDecoder) Remaining() int {
	return len(d.raw) - d.offset
}

func (d *BinaryDecoder) GetBytes(length int) []byte {
	value := d.raw[d.offset : d.offset+length]
	d.offset += length
	return value
}
