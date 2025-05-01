package model

import (
	"github.com/codecrafters-io/kafka-starter-go/app/api"
	describetopic "github.com/codecrafters-io/kafka-starter-go/app/api/describe_topic"
	"github.com/codecrafters-io/kafka-starter-go/app/api/fetch"
	"github.com/codecrafters-io/kafka-starter-go/app/config/constant"
	"github.com/codecrafters-io/kafka-starter-go/app/decoder"
)

type Message struct {
	MessageSize int32
	Header      api.RequestHeader
	Error       int16
	RequestBody any
}

func (m *Message) FromRawRequest(req *api.RawRequest) (*Message, error) {
	dec := &decoder.BinaryDecoder{}
	dec.Init(req.Payload)

	// parse request header
	var reqHeader api.RequestHeader
	if err := reqHeader.DecodeV2(dec); err != nil {
		return nil, err
	}

	m.MessageSize = int32(len(req.Payload))
	m.Header = reqHeader
	if m.Header.ApiVersion < 0 || m.Header.ApiVersion > 4 {
		m.Error = constant.ErrorUnsupportedVersion
	}

	// parse request body
	switch reqHeader.ApiKey {
	case constant.DescribeTopicPartitions:
		reqBody := describetopic.DescribeTopicPartitionsRequestBody{}
		if err := reqBody.DecodeV0(dec); err != nil {
			return nil, err
		}
		m.RequestBody = reqBody
	case constant.Fetch:
		reqBody := fetch.FetchRequestBody{}
		if err := reqBody.DecodeV16(dec); err != nil {
			return nil, err
		}
		m.RequestBody = reqBody
	}

	return m, nil
}
