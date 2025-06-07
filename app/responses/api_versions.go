package responses

import (
	"encoding/binary"
	"io"

	"github.com/nabinkhanal00/kafka/app/types"
)

type APIVersionsResponseV4 struct {
	ErrorCode      int16              `desc:"error_code"`
	APIKeys        []APIKey           `desc:"api_keys"`
	ThrottleTimeMS int32              `desc:"throttle_time_ms"`
	TaggedFields   types.TaggedFields `desc:"_tagged_fields"`
}
type APIKey struct {
	Key          int16              `desc:"api_key"`
	MinVersion   int16              `desc:"min_version"`
	MaxVersion   int16              `desc:"max_version"`
	TaggedFields types.TaggedFields `desc:"_tagged_fields"`
}

func (r *APIVersionsResponseV4) Write(w io.Writer) error {
	if err := binary.Write(w, binary.BigEndian, r.ErrorCode); err != nil {
		return err
	}
	binary.Write(w, binary.BigEndian, uint8(len(r.APIKeys)+1))
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
