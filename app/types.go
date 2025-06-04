package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

type NullableString struct {
	Length int16
	Data   string
}

func ParseNullableString(r *bytes.Reader) (*NullableString, error) {
	ns := NullableString{}
	nsLengthBuf := make([]byte, 2)
	binary.Read(r, binary.BigEndian, nsLengthBuf)
	ns.Length = int16(binary.BigEndian.Uint16(nsLengthBuf))
	if ns.Length != -1 {
		characters := make([]byte, ns.Length)
		n, err := r.Read(characters)
		if err != nil {
			log.Errorf("Unable to read: %v\n", err)
		}
		if n != int(ns.Length) {
			log.Errorf("Invalid no of characters: Expected :%d Got: %d\n", ns.Length, n)
		}
		ns.Data = string(characters)
	}
	return &ns, nil
}
func (ns *NullableString) Write(w io.Writer) error {
	nsLengthBuf := make([]byte, 2)
	binary.BigEndian.PutUint16(nsLengthBuf, uint16(ns.Length))
	_, err := w.Write(nsLengthBuf)
	if err != nil {
		return err
	}
	_, err = w.Write([]byte(ns.Data))
	return err
}

// TaggedField represents a single tagged field (tag ID + value)
type TaggedField struct {
	TagID uint64
	Value []byte
}

// TaggedFields holds multiple tagged fields
type TaggedFields struct {
	Fields map[uint64][]byte
}

// ReadUVarint reads an unsigned varint from the given reader
func ReadUVarint(r io.ByteReader) (uint64, error) {
	return binary.ReadUvarint(r)
}

// ParseTaggedFields parses tagged fields from a Kafka message
func ParseTaggedFields(r *bytes.Reader) (*TaggedFields, error) {
	fieldCount, err := ReadUVarint(r)
	if err != nil {
		return nil, fmt.Errorf("error reading tagged field count: %w", err)
	}

	tags := &TaggedFields{Fields: make(map[uint64][]byte)}

	for i := uint64(0); i < fieldCount; i++ {
		tagID, err := ReadUVarint(r)
		if err != nil {
			return nil, fmt.Errorf("error reading tag ID: %w", err)
		}

		length, err := ReadUVarint(r)
		if err != nil {
			return nil, fmt.Errorf("error reading tag length: %w", err)
		}

		value := make([]byte, length)
		if _, err := io.ReadFull(r, value); err != nil {
			return nil, fmt.Errorf("error reading tag value: %w", err)
		}

		tags.Fields[tagID] = value
	}

	return tags, nil
}

// WriteTaggedFields writes tagged fields to a buffer
func (t *TaggedFields) Write(w io.Writer) error {
	buf := &bytes.Buffer{}

	// Write number of tagged fields
	if err := writeUVarint(buf, uint64(len(t.Fields))); err != nil {
		return err
	}

	for tagID, value := range t.Fields {
		if err := writeUVarint(buf, tagID); err != nil {
			return err
		}
		if err := writeUVarint(buf, uint64(len(value))); err != nil {
			return err
		}
		if _, err := buf.Write(value); err != nil {
			return err
		}
	}

	_, err := w.Write(buf.Bytes())
	return err
}

func writeUVarint(w io.Writer, x uint64) error {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], x)
	_, err := w.Write(buf[:n])
	return err
}
