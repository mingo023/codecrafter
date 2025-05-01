package api

type Message struct {
	MessageSize int32
	Header      RequestHeader
	Error       int16
	RequestBody any
}
