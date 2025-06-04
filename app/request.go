package main

import (
	"bytes"
	"encoding/binary"
	"io"
)

type RequestHeader interface {
	Write(io.Writer) error
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

func (rh *RequestHeaderV2) Write(w io.Writer) error {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, rh.RequestAPIKey)
	binary.Write(&buf, binary.BigEndian, rh.RequestAPIVersion)
	binary.Write(&buf, binary.BigEndian, rh.CorrelationID)
	err := rh.ClientID.Write(&buf)
	if err != nil {
		return err
	}
	err = rh.TaggedFields.Write(&buf)
	if err != nil {
		return err
	}
	_, err = w.Write(buf.Bytes())
	return err
}

func ParseRequestHeader(r *bytes.Reader) RequestHeader {
	rh := ParseRequestHeaderV2(r)
	return rh
}

func ParseRequestHeaderV2(buf *bytes.Reader) *RequestHeaderV2 {
	rh := RequestHeaderV2{}
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
	return &rh
}

type Request struct {
	MessageSize int32         `desc:"message_size"`
	Header      RequestHeader `desc:"request_header"`
	Data        []byte        `desc:"data"`
}

func MarshallRequest(r Request) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, r.MessageSize)
	r.Header.Write(&buf)
	binary.Write(&buf, binary.BigEndian, r.Data)
	return buf.Bytes()
}
func UnmarshallRequest(b []byte) (Request, error) {
	buf := bytes.NewReader(b)
	req := Request{}
	req.Header = &RequestHeaderV2{}
	binary.Read(buf, binary.BigEndian, &req.MessageSize)
	req.Header = ParseRequestHeader(buf)
	req.Data, _ = io.ReadAll(buf)
	return req, nil
}
