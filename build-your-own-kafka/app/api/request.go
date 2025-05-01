package api

type RawRequest struct {
	RawBytes []byte
	Payload  []byte
}

func (r *RawRequest) From(msgSizeBytes, bodyBytes []byte) *RawRequest {
	r.RawBytes = append(msgSizeBytes, bodyBytes...)
	r.Payload = bodyBytes

	return r
}
