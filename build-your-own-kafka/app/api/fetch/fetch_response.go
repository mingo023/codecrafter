package fetch

import (
	"fmt"

	"github.com/codecrafters-io/kafka-starter-go/app/api"
	clustermetadata "github.com/codecrafters-io/kafka-starter-go/app/cluster_metadata"
	"github.com/codecrafters-io/kafka-starter-go/app/encoder"
	"github.com/google/uuid"
)

type FetchResponse struct {
	Header api.ResponseHeader
	Body   FetchResponseBody
}

type FetchResponseBody struct {
	ThrottleTime int32
	SessionID    int32
	ErrorCode    int16
	Responses    []FetchResponseTopic
}

type FetchResponseTopic struct {
	TopicID    uuid.UUID
	Partitions []FetchResponsePartition
}

type FetchResponsePartition struct {
	PartitionIndex       int32
	ErrorCode            int16
	HighWatermark        int64
	LastStableOffset     int64
	LogStartOffset       int64
	AbortedTransactions  []AbortedTransaction
	PreferredReadReplica int32
	Records              []clustermetadata.RecordBatch
}

type AbortedTransaction struct {
	ProducerID  int64
	FirstOffset int64
}

func (r *FetchResponse) Encode(enc *encoder.BinaryEncoder) error {
	if err := r.Header.EncodeV1(enc); err != nil {
		fmt.Println("Error encoding header:", err)
		return err
	}

	if err := r.Body.Encode(enc); err != nil {
		fmt.Println("Error encoding body:", err)
		return err
	}

	return nil
}

func (b *FetchResponseBody) Encode(enc *encoder.BinaryEncoder) error {
	enc.PutInt32(b.ThrottleTime)
	enc.PutInt16(b.ErrorCode)
	enc.PutInt32(b.SessionID)

	enc.PutCompactArrayLen(len(b.Responses))
	for _, topic := range b.Responses {
		if err := topic.Encode(enc); err != nil {
			fmt.Println("Error encoding topic:", err)
			return err
		}
	}
	enc.PutEmptyTaggedFieldArray()

	return nil
}

func (t *FetchResponseTopic) Encode(enc *encoder.BinaryEncoder) error {
	uuidBytes, _ := t.TopicID.MarshalBinary()
	enc.PutRawBytes(uuidBytes)

	enc.PutCompactArrayLen(len(t.Partitions))
	for _, partition := range t.Partitions {
		if err := partition.Encode(enc); err != nil {
			fmt.Println("Error encoding partition:", err)
			return err
		}
	}
	enc.PutEmptyTaggedFieldArray()

	return nil
}

func (p *FetchResponsePartition) Encode(enc *encoder.BinaryEncoder) error {
	enc.PutInt32(p.PartitionIndex)
	enc.PutInt16(p.ErrorCode)
	enc.PutInt64(p.HighWatermark)
	enc.PutInt64(p.LastStableOffset)
	enc.PutInt64(p.LogStartOffset)
	enc.PutCompactArrayLen(len(p.AbortedTransactions))
	for _, transaction := range p.AbortedTransactions {
		if err := transaction.Encode(enc); err != nil {
			fmt.Println("Error encoding transaction:", err)
			return err
		}
	}
	enc.PutInt32(p.PreferredReadReplica)

	// Clone the encoder and calculate the size of the record batches
	clonedEnc := enc.Clone()
	recordBatchSize := 0
	for _, record := range p.Records {
		if err := record.Encode(clonedEnc); err != nil {
			fmt.Println("Error encoding record:", err)
			return err
		}
		recordBatchSize += int(record.BatchLength)
	}

	// Handle real encoding of the record batches
	enc.PutCompactArrayLen(recordBatchSize)
	for _, record := range p.Records {
		if err := record.Encode(enc); err != nil {
			fmt.Println("Error encoding record:", err)
			return err
		}
	}

	enc.PutEmptyTaggedFieldArray()

	return nil
}

func (a *AbortedTransaction) Encode(enc *encoder.BinaryEncoder) error {
	enc.PutInt64(a.ProducerID)
	enc.PutInt64(a.FirstOffset)

	return nil
}
