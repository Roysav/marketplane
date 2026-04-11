package extension

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/roysav/marketplane/api/gen"
)

type manifestFile struct {
	Records []json.RawMessage `json:"records"`
}

// LoadRecords loads schema records from a JSON file or a directory of JSON files.
func LoadRecords(path string) ([]*pb.Record, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	if info.IsDir() {
		matches, err := filepath.Glob(filepath.Join(path, "*.json"))
		if err != nil {
			return nil, err
		}
		sort.Strings(matches)

		var records []*pb.Record
		for _, match := range matches {
			fileRecords, err := loadRecordsFromFile(match)
			if err != nil {
				return nil, err
			}
			records = append(records, fileRecords...)
		}
		return records, nil
	}

	return loadRecordsFromFile(path)
}

// Installer applies extension schema records through the RecordService.
type Installer struct {
	Records pb.RecordServiceClient
}

var ErrImmutableRecord = errors.New("schema record is immutable")

// Apply creates missing records and verifies existing immutable records match.
func (i *Installer) Apply(ctx context.Context, records []*pb.Record) error {
	if i == nil || i.Records == nil {
		return errors.New("record client is required")
	}

	for _, rec := range records {
		if err := i.applyRecord(ctx, rec); err != nil {
			return err
		}
	}

	return nil
}

func (i *Installer) applyRecord(ctx context.Context, rec *pb.Record) error {
	if rec == nil || rec.TypeMeta == nil || rec.ObjectMeta == nil {
		return errors.New("record type metadata and object metadata are required")
	}

	_, err := i.Records.Create(ctx, &pb.CreateRequest{Record: rec})
	if err == nil {
		return nil
	}
	if status.Code(err) != codes.AlreadyExists {
		return err
	}

	typeStr := fmt.Sprintf("%s/%s/%s", rec.TypeMeta.GetGroup(), rec.TypeMeta.GetVersion(), rec.TypeMeta.GetKind())
	current, err := i.Records.Get(ctx, &pb.GetRequest{
		Type:       typeStr,
		Tradespace: rec.ObjectMeta.GetTradespace(),
		Name:       rec.ObjectMeta.GetName(),
	})
	if err != nil {
		return err
	}
	if recordsEqual(current.Record, rec) {
		return nil
	}

	return fmt.Errorf("%w: %s %s/%s already exists with different contents; create a new version instead",
		ErrImmutableRecord,
		typeStr,
		rec.ObjectMeta.GetTradespace(),
		rec.ObjectMeta.GetName(),
	)
}

func loadRecordsFromFile(path string) ([]*pb.Record, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	if records, err := parseManifestRecords(data); err == nil {
		return records, nil
	}

	if records, err := parseRecordArray(data); err == nil {
		return records, nil
	}

	record, err := parseRecord(data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse %s: %w", path, err)
	}

	return []*pb.Record{record}, nil
}

func parseManifestRecords(data []byte) ([]*pb.Record, error) {
	var manifest manifestFile
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, err
	}
	if len(manifest.Records) == 0 {
		return nil, errors.New("manifest has no records")
	}

	records := make([]*pb.Record, 0, len(manifest.Records))
	for _, raw := range manifest.Records {
		rec, err := parseRecord(raw)
		if err != nil {
			return nil, err
		}
		records = append(records, rec)
	}
	return records, nil
}

func parseRecordArray(data []byte) ([]*pb.Record, error) {
	var rawRecords []json.RawMessage
	if err := json.Unmarshal(data, &rawRecords); err != nil {
		return nil, err
	}
	if len(rawRecords) == 0 {
		return nil, errors.New("record array is empty")
	}

	records := make([]*pb.Record, 0, len(rawRecords))
	for _, raw := range rawRecords {
		rec, err := parseRecord(raw)
		if err != nil {
			return nil, err
		}
		records = append(records, rec)
	}
	return records, nil
}

func parseRecord(data []byte) (*pb.Record, error) {
	rec := &pb.Record{}
	if err := protojson.Unmarshal(data, rec); err != nil {
		return nil, err
	}
	if rec.TypeMeta == nil || rec.ObjectMeta == nil {
		return nil, errors.New("record is missing typeMeta or objectMeta")
	}
	if strings.TrimSpace(rec.ObjectMeta.GetName()) == "" {
		return nil, errors.New("record name is required")
	}
	return rec, nil
}

func recordsEqual(current, desired *pb.Record) bool {
	if current == nil || desired == nil {
		return current == desired
	}
	if current.TypeMeta == nil || desired.TypeMeta == nil || current.ObjectMeta == nil || desired.ObjectMeta == nil {
		return false
	}

	if current.TypeMeta.GetGroup() != desired.TypeMeta.GetGroup() ||
		current.TypeMeta.GetVersion() != desired.TypeMeta.GetVersion() ||
		current.TypeMeta.GetKind() != desired.TypeMeta.GetKind() {
		return false
	}

	if current.ObjectMeta.GetName() != desired.ObjectMeta.GetName() ||
		current.ObjectMeta.GetTradespace() != desired.ObjectMeta.GetTradespace() {
		return false
	}

	if !reflect.DeepEqual(current.ObjectMeta.GetLabels(), desired.ObjectMeta.GetLabels()) {
		return false
	}
	if !reflect.DeepEqual(current.ObjectMeta.GetAnnotations(), desired.ObjectMeta.GetAnnotations()) {
		return false
	}
	if !reflect.DeepEqual(structToMap(current.Spec), structToMap(desired.Spec)) {
		return false
	}
	if !reflect.DeepEqual(structToMap(current.Status), structToMap(desired.Status)) {
		return false
	}

	return true
}

func structToMap(s *structpb.Struct) map[string]any {
	if s == nil {
		return nil
	}
	return s.AsMap()
}
