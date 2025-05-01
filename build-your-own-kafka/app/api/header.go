package api

import (
	"github.com/codecrafters-io/kafka-starter-go/app/config/constant"
	"github.com/codecrafters-io/kafka-starter-go/app/decoder"
	"github.com/codecrafters-io/kafka-starter-go/app/encoder"
)

type RequestHeader struct {
	ApiKey        constant.ApiKey
	ApiVersion    int16
	CorrelationId int32
	ClientId      string
}

func (r *RequestHeader) DecodeV2(dec *decoder.BinaryDecoder) error {
	r.ApiKey = constant.ApiKey(dec.GetInt16())
	r.ApiVersion = dec.GetInt16()
	r.CorrelationId = dec.GetInt32()
	r.ClientId = dec.GetString()

	dec.GetEmptyTaggedFieldArray()

	return nil
}

type ResponseHeader struct {
	CorrelationId int32
}

func (r *ResponseHeader) EncodeV0(enc *encoder.BinaryEncoder) error {
	enc.PutInt32(r.CorrelationId)

	return nil
}

func (r *ResponseHeader) EncodeV1(enc *encoder.BinaryEncoder) error {
	enc.PutInt32(r.CorrelationId)
	enc.PutEmptyTaggedFieldArray()

	return nil
}
