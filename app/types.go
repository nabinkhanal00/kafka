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
	var length int16
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return nil, fmt.Errorf("unable to read nullable string length: %w", err)
	}
	ns := NullableString{Length: length}
	if length != -1 {
		characters := make([]byte, length)
		if n, err := io.ReadFull(r, characters); err != nil {
			return nil, fmt.Errorf("unable to read nullable string data: %w", err)
		} else if n != int(length) {
			return nil, fmt.Errorf("invalid number of characters: expected %d, got %d", length, n)
		}
		ns.Data = string(characters)
	}
	return &ns, nil
}

func (ns *NullableString) Write(w io.Writer) error {
	if err := binary.Write(w, binary.BigEndian, ns.Length); err != nil {
		return err
	}
	if ns.Length > 0 {
		_, err := w.Write([]byte(ns.Data))
		return err
	}
	return nil
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
