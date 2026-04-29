// Package schemas exposes embedded MetaRecord JSON assets for the
// polymarket-order-controller. Consumers can turn these assets into
// core/v1/MetaRecord records at startup or in tests.
package schemas

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"path"
	"strings"

	"github.com/roysav/marketplane/pkg/record"
)

//go:embed *.json
var files embed.FS

// SystemTradespace is the tradespace that holds the built-in MetaRecords.
// The polymarket-order-controller registers its MetaRecords there so they are
// visible to every user tradespace via SyncMetaRecords.
const SystemTradespace = "system"

// Assets returns the raw embedded JSON payloads keyed by file base name
// (without the .json suffix). Useful for tests that want to load the payload
// directly without building a record.
func Assets() (map[string][]byte, error) {
	out := make(map[string][]byte)
	err := fs.WalkDir(files, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if path.Ext(p) != ".json" {
			return nil
		}
		data, err := files.ReadFile(p)
		if err != nil {
			return err
		}
		name := strings.TrimSuffix(path.Base(p), ".json")
		out[name] = data
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// MetaRecords returns the schema assets parsed into *record.Record values of
// type core/v1/MetaRecord. Records are placed in SystemTradespace with
// metadata.name = "<group>-<version>-<kind>" (lowercased). Callers may
// override metadata before persisting.
func MetaRecords() ([]*record.Record, error) {
	assets, err := Assets()
	if err != nil {
		return nil, err
	}

	records := make([]*record.Record, 0, len(assets))
	for name, raw := range assets {
		spec := map[string]any{}
		if err := json.Unmarshal(raw, &spec); err != nil {
			return nil, fmt.Errorf("parse schema %s: %w", name, err)
		}

		group, _ := spec["group"].(string)
		version, _ := spec["version"].(string)
		kind, _ := spec["kind"].(string)
		if group == "" || version == "" || kind == "" {
			return nil, fmt.Errorf("schema %s: group/version/kind required", name)
		}

		records = append(records, &record.Record{
			Type: record.MetaRecordType,
			ObjectMeta: record.ObjectMeta{
				Name:       strings.ToLower(fmt.Sprintf("%s-%s-%s", group, version, kind)),
				Tradespace: SystemTradespace,
			},
			Spec: spec,
		})
	}
	return records, nil
}
