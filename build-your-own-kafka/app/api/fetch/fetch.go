package fetch

import (
	"github.com/codecrafters-io/kafka-starter-go/app/api"
	clustermetadata "github.com/codecrafters-io/kafka-starter-go/app/cluster_metadata"
	"github.com/codecrafters-io/kafka-starter-go/app/config/constant"
)

func PrepareFetchResponse(msg *api.Message) FetchResponse {
	resp := FetchResponse{
		Header: api.ResponseHeader{
			CorrelationId: msg.Header.CorrelationId,
		},
		Body: FetchResponseBody{
			ThrottleTime: 0,
			SessionID:    0,
			ErrorCode:    constant.NoError,
		},
	}

	req := msg.RequestBody.(FetchRequestBody)
	topicResponses := make([]FetchResponseTopic, len(req.Topics))
	clusterMetadata := clustermetadata.GetClusterMetadata("__cluster_metadata", 0)
	for i, topic := range req.Topics {
		partitions := []FetchResponsePartition{}
		topicRec := clusterMetadata.GetTopicByID(topic.TopicID)
		if topicRec == nil {
			partitions = append(partitions, FetchResponsePartition{
				ErrorCode: constant.ErrorUnknownTopic,
			})
		} else {
			for _, partition := range topic.Partitions {
				partitionMetadata := clustermetadata.GetClusterMetadata(topicRec.TopicName, partition.PartitionIndex)
				partitions = append(partitions, FetchResponsePartition{
					PartitionIndex:       partition.PartitionIndex,
					ErrorCode:            0,
					HighWatermark:        0,
					LastStableOffset:     0,
					LogStartOffset:       0,
					AbortedTransactions:  nil,
					PreferredReadReplica: 0,
					Records:              partitionMetadata.RecordBatches,
				})
			}
		}

		topicResponses[i] = FetchResponseTopic{
			TopicID:    topic.TopicID,
			Partitions: partitions,
		}
	}

	resp.Body.Responses = topicResponses

	return resp
}
