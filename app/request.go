package main

import (
	"bytes"
	"encoding/binary"
	"io"
)

type RequestHeader interface {
	MarshalBinary() []byte
	UnmarshalBinary(*bytes.Reader)
}

type RequestHeaderV1 struct {
}

type RequestHeaderV2 struct {
	RequestAPIKey     int16          `desc:"request_api_key"`
	RequestAPIVersion int16          `desc:"request_api_version"`
	CorrelationID     int32          `desc:"correlation_id"`
	ClientID          NullableString `desc:"client_id"`
	TaggedFields      TaggedFields   `desc:"_tagged_fields"`
}

func (rh *RequestHeaderV2) MarshalBinary() []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, rh.RequestAPIKey)
	binary.Write(&buf, binary.BigEndian, rh.RequestAPIVersion)
	binary.Write(&buf, binary.BigEndian, rh.CorrelationID)
	rh.ClientID.Write(&buf)
	rh.TaggedFields.Write(&buf)
	return buf.Bytes()
}
func (rh *RequestHeaderV2) UnmarshalBinary(buf *bytes.Reader) {
	binary.Read(buf, binary.BigEndian, &rh.RequestAPIKey)
	binary.Read(buf, binary.BigEndian, &rh.RequestAPIVersion)
	binary.Read(buf, binary.BigEndian, &rh.CorrelationID)
	ci, err := ParseNullableString(buf)
	if err != nil {
		log.Errorf("Cannot parse nullable string:: %v\n", err)
	}
	rh.ClientID = *ci
	tfs, err := ParseTaggedFields(buf)
	if err != nil {
		log.Errorf("Errro: %v\n", err)
	}
	rh.TaggedFields = *tfs

}

type Request struct {
	MessageSize int32         `desc:"message_size"`
	Header      RequestHeader `desc:"request_header"`
	Data        []byte        `desc:"data"`
}

func MarshallRequest(r Request) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, r.MessageSize)
	binary.Write(&buf, binary.BigEndian, r.Header.MarshalBinary())
	binary.Write(&buf, binary.BigEndian, r.Data)
	return buf.Bytes()
}
func UnmarshallRequest(b []byte) (Request, error) {
	buf := bytes.NewReader(b)
	req := Request{}
	req.Header = &RequestHeaderV2{}
	binary.Read(buf, binary.BigEndian, &req.MessageSize)
	req.Header.UnmarshalBinary(buf)
	req.Data, _ = io.ReadAll(buf)
	return req, nil
}
