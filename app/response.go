package app

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/nabinkhanal00/kafka/app/types"
)

type ResponseHeader interface {
	Write(io.Writer) error
}
type ResponseBody interface {
	Write(io.Writer) error
}

type ResponseHeaderV0 struct {
	CorrelationID int32 `desc:"correlation_id"`
}

func (rh *ResponseHeaderV0) Write(w io.Writer) error {
	return binary.Write(w, binary.BigEndian, rh.CorrelationID)
}

type ResponseHeaderV1 struct {
	CorrelationID int32              `desc:"correlation_id"`
	TaggedFields  types.TaggedFields `desc:"_tagged_fields"`
}

func (rh *ResponseHeaderV1) Write(w io.Writer) error {
	if err := binary.Write(w, binary.BigEndian, rh.CorrelationID); err != nil {
		return err
	}
	return rh.TaggedFields.Write(w)
}

type ResponseHeaderV2 struct {
}

type Response struct {
	MessageSize int32          `desc:"message_size"`
	Header      ResponseHeader `desc:"response_header"`
	Body        ResponseBody   `desc:"data"`
}

func MarshallResponse(r Response) []byte {
	buf := bytes.Buffer{}
	binary.Write(&buf, binary.BigEndian, r.MessageSize)
	r.Header.Write(&buf)
	r.Body.Write(&buf)
	bufBytes := buf.Bytes()
	binary.BigEndian.PutUint32(bufBytes[0:4], uint32(len(bufBytes)-4))
	return bufBytes
}
