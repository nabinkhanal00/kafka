package main

import (
	"bytes"
	"encoding/binary"
)

type ResponseHeader interface {
	MarshalBinary() []byte
}

type ResponseHeaderV0 struct {
	CorrelationID int32 `desc:"correlation_id"`
}

func (rh *ResponseHeaderV0) MarshalBinary() []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, rh.CorrelationID)
	return buf.Bytes()
}

type ResponseHeaderV1 struct {
}

type ResponseHeaderV2 struct {
}

type Response struct {
	MessageSize int32          `desc:"message_size"`
	Header      ResponseHeader `desc:"response_header"`
	Data        []byte         `desc:"data"`
}

func MarshallResponse(r Response) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, r.MessageSize)
	binary.Write(&buf, binary.BigEndian, r.Header.MarshalBinary())
	binary.Write(&buf, binary.BigEndian, r.Data)
	return buf.Bytes()
}
