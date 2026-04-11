package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

type rowEnvelope struct {
	Spec json.RawMessage `json:"spec"`
}

// DecodeRowSpec decodes the record spec stored in a row.
//
// Rows created through the RecordService store data as {"spec": ..., "status": ...}.
// For backward compatibility with older direct row inserts, raw JSON specs are
// also accepted.
func DecodeRowSpec(row *Row, dest any) error {
	if row == nil {
		return errors.New("row is nil")
	}

	data := strings.TrimSpace(row.Data)
	if data == "" {
		return errors.New("row data is empty")
	}

	var env rowEnvelope
	if err := json.Unmarshal([]byte(data), &env); err == nil && len(env.Spec) > 0 {
		if err := json.Unmarshal(env.Spec, dest); err != nil {
			return fmt.Errorf("failed to decode row spec: %w", err)
		}
		return nil
	}

	if err := json.Unmarshal([]byte(data), dest); err != nil {
		return fmt.Errorf("failed to decode row spec: %w", err)
	}

	return nil
}

// DecodeRowSpecMap decodes the row spec into a generic map.
func DecodeRowSpecMap(row *Row) (map[string]any, error) {
	var spec map[string]any
	if err := DecodeRowSpec(row, &spec); err != nil {
		return nil, err
	}
	return spec, nil
}
