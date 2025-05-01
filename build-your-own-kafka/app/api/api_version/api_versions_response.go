package apiversion

import (
	"github.com/codecrafters-io/kafka-starter-go/app/api"
	"github.com/codecrafters-io/kafka-starter-go/app/config/constant"
	"github.com/codecrafters-io/kafka-starter-go/app/encoder"
)

type ApiVersionsResponse struct {
	Header api.ResponseHeader
	Body   ApiVersionsResponseBody
}

type ApiVersionsResponseBody struct {
	ErrorCode    int16
	ApiVersions  []ApiVersion
	ThrottleTime int32
}

func (b *ApiVersionsResponseBody) Encode(enc *encoder.BinaryEncoder) error {
	enc.PutInt16(b.ErrorCode)
	if b.ErrorCode != constant.NoError {
		return nil
	}

	enc.PutCompactArrayLen(len(b.ApiVersions))
	for _, v := range b.ApiVersions {
		if err := v.Encode(enc); err != nil {
			return err
		}
	}

	enc.PutInt32(b.ThrottleTime)
	enc.PutEmptyTaggedFieldArray()

	return nil
}

type ApiVersion struct {
	ApiKey     int16
	MinVersion int16
	MaxVersion int16
}

func (a *ApiVersion) Encode(enc *encoder.BinaryEncoder) error {
	enc.PutInt16(a.ApiKey)
	enc.PutInt16(a.MinVersion)
	enc.PutInt16(a.MaxVersion)
	enc.PutEmptyTaggedFieldArray()
	return nil
}

func (a *ApiVersionsResponse) Encode(enc *encoder.BinaryEncoder) error {
	if err := a.Header.EncodeV0(enc); err != nil {
		return err
	}

	if err := a.Body.Encode(enc); err != nil {
		return err
	}
	return nil
}
