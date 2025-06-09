package responses

import (
	"encoding/binary"
	"io"

	"github.com/nabinkhanal00/kafka/app/types"
)

type Node struct {
	NodeID int32 `desc:"node_id"`
}
type Nodes []Node

func (n *Nodes) Write(w io.Writer) error {
	if err := types.WriteUvarint(w, uint64(len(*n)+1)); err != nil {
		return err
	}
	for _, node := range *n {
		if err := binary.Write(w, binary.BigEndian, node.NodeID); err != nil {
			return err
		}
	}
	return nil
}

type DescribeTopicPartitionsV0 struct {
	ThrottleTime int32              `desc:"throttle_time"`
	Topics       []Topic            `desc:"topics"`
	NextCursor   Cursor             `desc:"next_cursor"`
	TaggedFields types.TaggedFields `desc:"_tagged_fields"`
}

type Cursor struct {
	TopicName      types.CompactString `desc:"topic_name"`
	PartitionIndex int32               `desc:"partition_index"`
	TaggedFields   types.TaggedFields  `desc:"_tagged_fields"`
}

func (c *Cursor) Write(w io.Writer) error {
	if err := c.TopicName.Write(w); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, c.PartitionIndex); err != nil {
		return err
	}
	return c.TaggedFields.Write(w)
}

type Topic struct {
	ErrorCode                 int16               `desc:"error_code"`
	TopicName                 types.CompactString `desc:"topic_name"`
	TopicID                   [16]byte            `desc:"topic_id"`
	IsInternal                byte                `desc:"is_internal"`
	Partitions                Partitions          `desc:"partitions"`
	TopicAuthorizedOperations int32               `desc:"topic_authorized_operations"`
	TaggedFields              types.TaggedFields  `desc:"_tagged_fields"`
}

type Partitions struct {
	Partitions   []Partition        `desc:"partitions"`
	TaggedFields types.TaggedFields `desc:"_tagged_fields"`
}

func (p *Partitions) Write(w io.Writer) error {

	if err := types.WriteUvarint(w, uint64(len(p.Partitions)+1)); err != nil {
		return err
	}
	for _, partition := range p.Partitions {
		if err := partition.Write(w); err != nil {
			return err
		}
	}
	return p.TaggedFields.Write(w)
}

func (t *Topic) Write(w io.Writer) error {
	if err := binary.Write(w, binary.BigEndian, t.ErrorCode); err != nil {
		return err
	}
	if err := t.TopicName.Write(w); err != nil {
		return err
	}
	if _, err := w.Write(t.TopicID[:]); err != nil {
		return err
	}
	if _, err := w.Write([]byte{t.IsInternal}); err != nil {
		return err
	}
	if err := t.Partitions.Write(w); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, t.TopicAuthorizedOperations); err != nil {
		return err
	}
	return t.TaggedFields.Write(w)
}

type Partition struct {
	ErrorCode              int16 `desc:"error_code"`
	PartitionIndex         int32 `desc:"partition_index"`
	LeaderID               int32 `desc:"leader_id"`
	LeaderEpoch            int32 `desc:"leader_epoch"`
	ReplicaNodes           Nodes `desc:"replica_nodes"`
	ISRNodes               Nodes `desc:"isr_nodes"`
	EligibleLeaderReplicas Nodes `desc:"eligible_leader_replicas"`
	LastKnownELRs          Nodes `desc:"last_known_eligible_leader_replicas"`
	OfflineReplicas        Nodes `desc:"offline_replicas"`
}

func (p *Partition) Write(w io.Writer) error {

	if err := binary.Write(w, binary.BigEndian, p.ErrorCode); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, p.PartitionIndex); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, p.LeaderID); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, p.LeaderEpoch); err != nil {
		return err
	}
	if err := p.ReplicaNodes.Write(w); err != nil {
		return err
	}
	if err := p.ISRNodes.Write(w); err != nil {
		return err
	}
	if err := p.EligibleLeaderReplicas.Write(w); err != nil {
		return err
	}
	if err := p.LastKnownELRs.Write(w); err != nil {
		return err
	}
	return p.OfflineReplicas.Write(w)
}

func (r *DescribeTopicPartitionsV0) Write(w io.Writer) error {
	if err := binary.Write(w, binary.BigEndian, r.ThrottleTime); err != nil {
		return err
	}

	if err := types.WriteUvarint(w, uint64(len(r.Topics)+1)); err != nil {
		return err
	}
	for _, topic := range r.Topics {
		if err := topic.Write(w); err != nil {
			return err
		}
	}
	if err := r.NextCursor.Write(w); err != nil {
		return err
	}
	return r.TaggedFields.Write(w)
}
