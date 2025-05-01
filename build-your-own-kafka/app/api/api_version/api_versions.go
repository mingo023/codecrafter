package apiversion

import (
	"github.com/codecrafters-io/kafka-starter-go/app/api"
	"github.com/codecrafters-io/kafka-starter-go/app/config/constant"
)

func PrepareAPIVersionsResponse(msg *api.Message) ApiVersionsResponse {
	resp := ApiVersionsResponse{
		Header: api.ResponseHeader{CorrelationId: msg.Header.CorrelationId},
		Body: ApiVersionsResponseBody{
			ErrorCode: msg.Error,
		},
	}

	if msg.Error == constant.NoError {
		resp.Body.ApiVersions = []ApiVersion{
			{ApiKey: constant.ApiVersions, MinVersion: 0, MaxVersion: 5},
			{ApiKey: constant.DescribeTopicPartitions, MinVersion: 0, MaxVersion: 11},
			{ApiKey: constant.Fetch, MinVersion: 0, MaxVersion: 17},
		}
	}

	return resp
}
