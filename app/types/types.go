package types

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

type NullableString string
type CompactString string

type Integer interface {
	~int8 | ~int16 | ~int32 | ~int64 |
		~uint8 | ~uint16 | ~uint32 | ~uint64
}

type ElementType interface {
	Integer | CompactString | NullableString | TaggedFields
}

func Parse[T ElementType](r *bytes.Reader) (*T, error) {
	var v T
	switch any(v).(type) {
	case int8, int16, int32, int64, uint8, uint16, uint32, uint64:
		err := binary.Read(r, binary.BigEndian, &v)
		return &v, err
	case NullableString:
		ns, err := ParseNullableString(r)
		return any(ns).(*T), err
	case CompactString:
		cs, err := ParseCompactString(r)
		return any(cs).(*T), err
	case TaggedFields:
		tfs, err := ParseTaggedFields(r)
		return any(tfs).(*T), err
	default:
		return nil, fmt.Errorf("invalid type: %T", v)
	}
}

func ParseCompactString(r *bytes.Reader) (*CompactString, error) {
	var length uint64
	var err error
	if length, err = binary.ReadUvarint(r); err != nil {
		return nil, fmt.Errorf("unable to read compact string length: %w", err)
	}
	var cs CompactString
	length -= 1
	if length != 0 {
		characters := make([]byte, length)
		if n, err := io.ReadFull(r, characters); err != nil {
			return nil, fmt.Errorf("unable to read compact string data: %w", err)
		} else if n != int(length) {
			return nil, fmt.Errorf("invalid number of characters: expected %d, got %d", length, n)
		}
		cs = CompactString(characters)
	}
	return &cs, nil
}

func ParseNullableString(r *bytes.Reader) (*NullableString, error) {
	var length int16
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return nil, fmt.Errorf("unable to read nullable string length: %w", err)
	}
	var ns NullableString
	if length != -1 {
		characters := make([]byte, length)
		if n, err := io.ReadFull(r, characters); err != nil {
			return nil, fmt.Errorf("unable to read nullable string data: %w", err)
		} else if n != int(length) {
			return nil, fmt.Errorf("invalid number of characters: expected %d, got %d", length, n)
		}
		ns = NullableString(characters)
	}
	return &ns, nil
}

func (ns *NullableString) Write(w io.Writer) error {
	if err := binary.Write(w, binary.BigEndian, int32(len(*ns))); err != nil {
		return err
	}
	if len(*ns) > 0 {
		_, err := w.Write([]byte(*ns))
		return err
	}
	return nil
}
func (cs *CompactString) Write(w io.Writer) error {
	buf := [binary.MaxVarintLen64]byte{}
	n := binary.PutUvarint(buf[:], uint64(len(*cs)+1))
	_, err := w.Write(buf[:n])
	if err != nil {
		return err
	}
	if len(*cs) > 0 {
		_, err := w.Write([]byte(*cs))
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

// ReadUvarint reads an unsigned varint from the given reader
func ReadUvarint(r io.ByteReader) (uint64, error) {
	return binary.ReadUvarint(r)
}

// ReadVarint reads an signed varint from the given reader
func ReadVarint(r io.ByteReader) (int64, error) {
	return binary.ReadVarint(r)
}

// ParseTaggedFields parses tagged fields from a Kafka message
func ParseTaggedFields(r *bytes.Reader) (*TaggedFields, error) {
	fieldCount, err := ReadUvarint(r)
	if err != nil {
		return nil, fmt.Errorf("error reading tagged field count: %w", err)
	}

	tags := &TaggedFields{Fields: make(map[uint64][]byte)}

	for i := uint64(0); i < fieldCount; i++ {
		tagID, err := ReadUvarint(r)
		if err != nil {
			return nil, fmt.Errorf("error reading tag ID: %w", err)
		}

		length, err := ReadUvarint(r)
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
	if err := WriteUvarint(buf, uint64(len(t.Fields))); err != nil {
		return err
	}

	for tagID, value := range t.Fields {
		if err := WriteUvarint(buf, tagID); err != nil {
			return err
		}
		if err := WriteUvarint(buf, uint64(len(value))); err != nil {
			return err
		}
		if _, err := buf.Write(value); err != nil {
			return err
		}
	}

	_, err := w.Write(buf.Bytes())
	return err
}

func WriteUvarint(w io.Writer, x uint64) error {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], x)
	_, err := w.Write(buf[:n])
	return err
}

func WriteVarint(w io.Writer, x int64) error {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutVarint(buf[:], x)
	_, err := w.Write(buf[:n])
	return err
}
