package describetopic

import (
	"github.com/codecrafters-io/kafka-starter-go/app/api"
	clustermetadata "github.com/codecrafters-io/kafka-starter-go/app/cluster_metadata"
	"github.com/codecrafters-io/kafka-starter-go/app/config/constant"
	"github.com/google/uuid"
)

func PrepareDescribeTopicPartitionsResponse(msg *api.Message) DescribeTopicPartitionsResponse {
	clusterMetadata := clustermetadata.GetClusterMetadata("__cluster_metadata", 0)

	req := msg.RequestBody.(DescribeTopicPartitionsRequestBody)
	resp := DescribeTopicPartitionsResponse{
		Header: api.ResponseHeader{
			CorrelationId: msg.Header.CorrelationId,
		},
		Body: DescribeTopicPartitionsResponseV0ResponseBody{
			ThrottleTime: 0,
			Topics:       nil,
			Cursor:       Cursor{NextCursor: nil},
		},
	}

	for _, topic := range req.TopicNames {
		topicRecord := clusterMetadata.GetTopicByName(topic.Name)
		if topicRecord == nil {
			resp.Body.Topics = append(resp.Body.Topics, DescribeTopicPartitionsResponseV0Topic{
				ErrorCode:  constant.UnknownTopicOrPartition,
				ID:         uuid.UUID{},
				Name:       topic.Name,
				IsInternal: 0,
				Partitions: nil,
			})
		} else {
			partitionRecords := clusterMetadata.GetPartitionsByTopicId(topicRecord.TopicUUID)
			partitions := make([]Partition, 0, len(partitionRecords))
			for _, partition := range partitionRecords {
				partitions = append(partitions, Partition{
					ErrorCode:      constant.NoError,
					PartitionIndex: partition.PartitionID,
					LeaderID:       partition.Leader,
					LeaderEpoch:    partition.LeaderEpoch,
					ReplicaNodes:   partition.Replicas,
					ISRNodes:       partition.InSyncReplicas,
				})
			}

			resp.Body.Topics = append(resp.Body.Topics, DescribeTopicPartitionsResponseV0Topic{
				ErrorCode:  constant.NoError,
				Name:       topicRecord.TopicName,
				ID:         topicRecord.TopicUUID,
				IsInternal: 0,
				Partitions: partitions,
			})
		}
	}

	return resp
}
