package clustermetadata

import (
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/codecrafters-io/kafka-starter-go/app/decoder"
	"github.com/google/uuid"
)

const clusterMetadataLogFilePathPattern = "/tmp/kraft-combined-logs/%s-%s/00000000000000000000.log"

type ClusterMetadata struct {
	RecordBatches []RecordBatch
}

func GetClusterMetadata(topicName string, partitionId int32) ClusterMetadata {
	res := ClusterMetadata{
		RecordBatches: make([]RecordBatch, 0),
	}

	file, err := os.Open(
		fmt.Sprintf(clusterMetadataLogFilePathPattern, topicName, strconv.Itoa(int(partitionId))),
	)
	if err != nil {
		fmt.Println("Error opening file: ", err.Error())
		return res
	}

	bytes, err := io.ReadAll(file)
	if err != nil {
		return res
	}

	dec := &decoder.BinaryDecoder{}
	dec.Init(bytes)
	for dec.Remaining() > 0 {
		recordBatch := RecordBatch{}
		if err := recordBatch.Decode(dec); err != nil {
			fmt.Println("Error decoding record batch: ", err.Error())
			break
		}
		res.RecordBatches = append(res.RecordBatches, recordBatch)
	}

	return res
}

func (c *ClusterMetadata) GetTopicByName(topicName string) *TopicRecord {
	for _, recordBatch := range c.RecordBatches {
		for _, record := range recordBatch.Records {
			var clusterMetadataRecordVal ClusterMetadataRecordValue
			_ = clusterMetadataRecordVal.DecodeBytes(record.Value)
			if topicRecord, ok := clusterMetadataRecordVal.Data.(*TopicRecord); ok {
				if topicRecord.TopicName == topicName {
					return topicRecord
				}
			}
		}
	}

	return nil
}

func (c *ClusterMetadata) GetPartitionsByTopicId(topicId uuid.UUID) []*PartitionRecord {
	var partitions []*PartitionRecord
	for _, recordBatch := range c.RecordBatches {
		for _, record := range recordBatch.Records {
			var clusterMetadataRecordVal ClusterMetadataRecordValue
			_ = clusterMetadataRecordVal.DecodeBytes(record.Value)
			if partitionRecord, ok := clusterMetadataRecordVal.Data.(*PartitionRecord); ok {
				if partitionRecord.TopicUUID == topicId {
					partitions = append(partitions, partitionRecord)
				}
			}
		}
	}
	return partitions
}

func (c *ClusterMetadata) GetTopicByID(topicID uuid.UUID) *TopicRecord {
	for _, recordBatch := range c.RecordBatches {
		for _, record := range recordBatch.Records {
			var clusterMetadataRecordVal ClusterMetadataRecordValue
			_ = clusterMetadataRecordVal.DecodeBytes(record.Value)
			if topicRecord, ok := clusterMetadataRecordVal.Data.(*TopicRecord); ok {
				if topicRecord.TopicUUID == topicID {
					return topicRecord
				}
			}
		}
	}
	return nil
}
