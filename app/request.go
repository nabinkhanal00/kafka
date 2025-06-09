package app

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/nabinkhanal00/kafka/app/requests"
	"github.com/nabinkhanal00/kafka/app/types"
)

type RequestHeader interface {
	Write(io.Writer) error
	GetAPIKey() int16
}
type RequestBody interface {
	Write(io.Writer) error
}

type RequestHeaderV2 struct {
	RequestAPIKey     int16                `desc:"request_api_key"`
	RequestAPIVersion int16                `desc:"request_api_version"`
	CorrelationID     int32                `desc:"correlation_id"`
	ClientID          types.NullableString `desc:"client_id"`
	TaggedFields      types.TaggedFields   `desc:"_tagged_fields"`
}

func (rh *RequestHeaderV2) GetAPIKey() int16 {
	return rh.RequestAPIKey
}

func (rh *RequestHeaderV2) Write(w io.Writer) error {
	if err := binary.Write(w, binary.BigEndian, rh.RequestAPIKey); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, rh.RequestAPIVersion); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, rh.CorrelationID); err != nil {
		return err
	}
	if err := rh.ClientID.Write(w); err != nil {
		return err
	}
	return rh.TaggedFields.Write(w)
}

func ParseRequestHeader(r *bytes.Reader) (RequestHeader, error) {
	return ParseRequestHeaderV2(r)
}

func ParseRequestHeaderV2(r *bytes.Reader) (*RequestHeaderV2, error) {
	var rh RequestHeaderV2
	if err := binary.Read(r, binary.BigEndian, &rh.RequestAPIKey); err != nil {
		return nil, fmt.Errorf("cannot read api key: %w", err)
	}
	if err := binary.Read(r, binary.BigEndian, &rh.RequestAPIVersion); err != nil {
		return nil, fmt.Errorf("cannot read api version: %w", err)
	}
	if err := binary.Read(r, binary.BigEndian, &rh.CorrelationID); err != nil {
		return nil, fmt.Errorf("cannot read correlation id: %w", err)
	}
	ci, err := types.ParseNullableString(r)
	if err != nil {
		return nil, fmt.Errorf("cannot parse nullable string: %w", err)
	}
	rh.ClientID = *ci
	tfs, err := types.ParseTaggedFields(r)
	if err != nil {
		return nil, fmt.Errorf("cannot parse tagged fields: %w", err)
	}
	rh.TaggedFields = *tfs
	return &rh, nil
}

type Request struct {
	MessageSize int32         `desc:"message_size"`
	Header      RequestHeader `desc:"request_header"`
	Body        RequestBody   `desc:"data"`
}

func MarshallRequest(r Request) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, r.MessageSize)
	r.Header.Write(&buf)
	r.Body.Write(&buf)
	return buf.Bytes()
}

func UnmarshallRequest(b []byte) (Request, error) {
	buf := bytes.NewReader(b)
	var req Request
	if err := binary.Read(buf, binary.BigEndian, &req.MessageSize); err != nil {
		return req, err
	}
	header, err := ParseRequestHeader(buf)
	if err != nil {
		return req, err
	}
	req.Header = header
	req.Body, err = ParseRequestBody(header, buf)
	return req, err
}

func ParseRequestBody(h RequestHeader, r *bytes.Reader) (RequestBody, error) {
	switch h.GetAPIKey() {
	case ApiVersions:
		return requests.ParseAPIVersionsV4(r)
	case DescribeTopicPartitions:
		return requests.ParseDescribeTopicPartitionsV0(r)
	default:
		return nil, nil
	}
}
