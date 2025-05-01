package describetopic

import (
	"github.com/codecrafters-io/kafka-starter-go/app/api"
	"github.com/codecrafters-io/kafka-starter-go/app/encoder"
	"github.com/google/uuid"
)

type DescribeTopicPartitionsResponse struct {
	Header api.ResponseHeader
	Body   DescribeTopicPartitionsResponseV0ResponseBody
}

type DescribeTopicPartitionsResponseV0ResponseBody struct {
	ThrottleTime int32
	Topics       []DescribeTopicPartitionsResponseV0Topic
	Cursor       Cursor
}

func (d *DescribeTopicPartitionsResponseV0ResponseBody) Encode(enc *encoder.BinaryEncoder) error {
	enc.PutInt32(d.ThrottleTime)
	enc.PutCompactArrayLen(len(d.Topics))
	for _, topic := range d.Topics {
		err := topic.Encode(enc)
		if err != nil {
			return err
		}
	}
	err := d.Cursor.Encode(enc)
	if err != nil {
		return err
	}
	enc.PutEmptyTaggedFieldArray()
	return nil
}

type Cursor struct {
	NextCursor *int8
}

func (c *Cursor) Encode(enc *encoder.BinaryEncoder) error {
	if c.NextCursor != nil {
		enc.PutInt8(*c.NextCursor)
	} else {
		enc.PutInt8(-1)
	}
	return nil
}

type DescribeTopicPartitionsResponseV0Topic struct {
	ErrorCode            int16
	Name                 string
	ID                   uuid.UUID
	IsInternal           byte
	Partitions           []Partition
	AuthorizedOperations int32
}

type Partition struct {
	ErrorCode       int16
	PartitionIndex  int32
	LeaderID        int32
	LeaderEpoch     int32
	ReplicaNodes    []int32
	ISRNodes        []int32
	ELRs            []int32 // eligible leader replicas
	LastKnownELRs   []int32
	OffLineReplicas []int32
}

func (d *DescribeTopicPartitionsResponseV0Topic) Encode(enc *encoder.BinaryEncoder) error {
	enc.PutInt16(d.ErrorCode)
	enc.PutCompactString(d.Name)
	uuidBytes, err := d.ID.MarshalBinary()
	if err != nil {
		return err
	}
	enc.PutRawBytes(uuidBytes)
	enc.PutInt8(int8(d.IsInternal))
	enc.PutCompactArrayLen(len(d.Partitions))
	for _, partition := range d.Partitions {
		if err = partition.Encode(enc); err != nil {
			return err
		}
	}
	enc.PutInt32(d.AuthorizedOperations)
	enc.PutEmptyTaggedFieldArray()
	return nil
}

func (p *Partition) Encode(enc *encoder.BinaryEncoder) error {
	enc.PutInt16(p.ErrorCode)
	enc.PutInt32(p.PartitionIndex)
	enc.PutInt32(p.LeaderID)
	enc.PutInt32(p.LeaderEpoch)
	enc.PutCompactInt32Array(p.ReplicaNodes)
	enc.PutCompactInt32Array(p.ISRNodes)
	enc.PutCompactInt32Array(p.ELRs)
	enc.PutCompactInt32Array(p.LastKnownELRs)
	enc.PutCompactInt32Array(p.OffLineReplicas)
	enc.PutEmptyTaggedFieldArray()
	return nil
}

func (d *DescribeTopicPartitionsResponse) Encode(enc *encoder.BinaryEncoder) error {
	err := d.Header.EncodeV1(enc)
	if err != nil {
		return err
	}
	err = d.Body.Encode(enc)
	if err != nil {
		return err
	}
	return nil
}
