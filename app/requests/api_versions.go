package requests

import (
	"bytes"
	"io"

	"github.com/nabinkhanal00/kafka/app/types"
)

type APIVersionsV4 struct {
	ClientSoftwareName    types.CompactString `desc:"client_software_name"`
	ClientSoftwareVersion types.CompactString `desc:"client_software_version"`
	TaggedFields          types.TaggedFields  `desc:"_tagged_fields"`
}

func (r *APIVersionsV4) Write(w io.Writer) error {
	if err := r.ClientSoftwareName.Write(w); err != nil {
		return err
	}
	if err := r.ClientSoftwareVersion.Write(w); err != nil {
		return err
	}
	return r.TaggedFields.Write(w)
}
func ParseAPIVersionsV4(r *bytes.Reader) (*APIVersionsV4, error) {
	clientSoftwareName, err := types.ParseCompactString(r)
	if err != nil {
		return nil, err
	}
	clientSoftwareVersion, err := types.ParseCompactString(r)
	if err != nil {
		return nil, err
	}
	taggedFields, err := types.ParseTaggedFields(r)

	if err != nil {
		return nil, err
	}

	return &APIVersionsV4{
		ClientSoftwareName:    *clientSoftwareName,
		ClientSoftwareVersion: *clientSoftwareVersion,
		TaggedFields:          *taggedFields,
	}, nil
}
