package main

import (
	"bytes"
	"encoding/binary"
	"io"
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
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, rh.CorrelationID)
	_, err := w.Write(buf.Bytes())
	return err
}

type ResponseHeaderV1 struct {
}

type ResponseHeaderV2 struct {
}

type Response struct {
	MessageSize int32          `desc:"message_size"`
	Header      ResponseHeader `desc:"response_header"`
	Body        ResponseBody   `desc:"data"`
}
type APIVersionsResponseV4 struct {
	ErrorCode      int16        `desc:"error_code"`
	APIKeys        []APIKey     `desc:"api_keys"`
	ThrottleTimeMS int32        `desc:"throttle_time_ms"`
	TaggedFields   TaggedFields `desc:"_tagged_fields"`
}
type APIKey struct {
	Key          int16        `desc:"api_key"`
	MinVersion   int16        `desc:"min_version"`
	MaxVersion   int16        `desc:"max_version"`
	TaggedFields TaggedFields `desc:"_tagged_fields"`
}

func (r *APIVersionsResponseV4) Write(w io.Writer) error {
	if err := binary.Write(w, binary.BigEndian, r.ErrorCode); err != nil {
		return err
	}
	binary.Write(w, binary.BigEndian, int32(len(r.APIKeys)))
	for _, apikey := range r.APIKeys {
		binary.Write(w, binary.BigEndian, apikey.Key)
		binary.Write(w, binary.BigEndian, apikey.MinVersion)
		binary.Write(w, binary.BigEndian, apikey.MaxVersion)
		apikey.TaggedFields.Write(w)
	}
	binary.Write(w, binary.BigEndian, r.ThrottleTimeMS)
	r.TaggedFields.Write(w)
	return nil
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
