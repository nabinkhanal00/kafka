package requests

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/nabinkhanal00/kafka/app/types"
)

type DescribeTopicPartitionsV0 struct {
	Topics                 []Topic            `desc:"topics"`
	ResponsePartitionLimit int32              `desc:"response_partition_limits"`
	Cursor                 int8               `desc:"cursor"`
	TaggedFields           types.TaggedFields `desc:"_tagged_fields"`
}

type Topic struct {
	Name         types.CompactString `desc:"name"`
	TaggedFields types.TaggedFields  `desc:"_tagged_fields"`
}

func (t *Topic) Write(w io.Writer) error {
	if err := t.Name.Write(w); err != nil {
		return err
	}
	return t.TaggedFields.Write(w)
}
func ParseTopic(r *bytes.Reader) (*Topic, error) {
	name, err := types.ParseCompactString(r)
	if err != nil {
		return nil, err
	}
	taggedFields, err := types.ParseTaggedFields(r)
	if err != nil {
		return nil, err
	}
	return &Topic{
		Name:         *name,
		TaggedFields: *taggedFields,
	}, nil
}

func ParseDescribeTopicPartitionsV0(r *bytes.Reader) (*DescribeTopicPartitionsV0, error) {
	numTopics, err := types.ReadUvarint(r)
	if err != nil {
		return nil, err
	}
	// the length of array is always 1 more than actual
	numTopics -= 1
	topics := []Topic{}
	for range numTopics {
		topic, err := ParseTopic(r)
		if err != nil {
			return nil, err
		}
		topics = append(topics, *topic)
	}
	responsePartitionLimit, err := types.Parse[int32](r)
	if err != nil {
		return nil, err
	}
	cursor, err := types.Parse[int8](r)
	if err != nil {
		return nil, err
	}
	taggedFields, err := types.Parse[types.TaggedFields](r)
	if err != nil {
		return nil, err
	}
	return &DescribeTopicPartitionsV0{
		Topics:                 topics,
		ResponsePartitionLimit: *responsePartitionLimit,
		Cursor:                 *cursor,
		TaggedFields:           *taggedFields,
	}, err

}

func (r *DescribeTopicPartitionsV0) Write(w io.Writer) error {
	if err := types.WriteUvarint(w, uint64(len(r.Topics))+1); err != nil {
		return err
	}
	for _, topic := range r.Topics {
		if err := topic.Write(w); err != nil {
			return err
		}
	}
	if err := binary.Write(w, binary.BigEndian, r.ResponsePartitionLimit); err != nil {
		return err
	}
	return r.TaggedFields.Write(w)
}
