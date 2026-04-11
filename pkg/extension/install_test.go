package extension

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/roysav/marketplane/api/gen"
	"github.com/roysav/marketplane/pkg/server"
	"github.com/roysav/marketplane/pkg/service"
	"github.com/roysav/marketplane/pkg/storage/sqlite"
)

func setupExtensionTestServer(t *testing.T) (pb.RecordServiceClient, func()) {
	t.Helper()

	rows, err := sqlite.New(context.Background(), "file:extension-test?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("failed to create sqlite storage: %v", err)
	}

	grpcServer := grpc.NewServer()
	server.New(service.New(service.Config{Rows: rows}), nil).Register(grpcServer)

	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		rows.Close()
		t.Fatalf("failed to listen: %v", err)
	}

	go grpcServer.Serve(lis)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		grpcServer.Stop()
		lis.Close()
		rows.Close()
		t.Fatalf("failed to connect: %v", err)
	}

	cleanup := func() {
		conn.Close()
		grpcServer.Stop()
		lis.Close()
		rows.Close()
	}

	return pb.NewRecordServiceClient(conn), cleanup
}

func TestLoadRecordsFromManifestFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "schema.json")

	err := os.WriteFile(path, []byte(`{
  "records": [
    {
      "typeMeta": {"group": "core", "version": "v1", "kind": "MetaRecord"},
      "objectMeta": {"name": "Thing.example", "tradespace": "default"},
      "spec": {"group": "example", "version": "v1", "kind": "Thing"}
    }
  ]
}`), 0o644)
	if err != nil {
		t.Fatalf("failed to write manifest: %v", err)
	}

	records, err := LoadRecords(path)
	if err != nil {
		t.Fatalf("LoadRecords failed: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if records[0].ObjectMeta.GetName() != "Thing.example" {
		t.Fatalf("unexpected record name: %s", records[0].ObjectMeta.GetName())
	}
}

func TestInstallerApplyCreateAndNoopOnExactMatch(t *testing.T) {
	recordsClient, cleanup := setupExtensionTestServer(t)
	defer cleanup()

	installer := &Installer{Records: recordsClient}

	initial := []*pb.Record{
		{
			TypeMeta: &pb.TypeMeta{
				Group:   "core",
				Version: "v1",
				Kind:    "MetaRecord",
			},
			ObjectMeta: &pb.ObjectMeta{
				Name:       "Thing.example",
				Tradespace: "default",
			},
			Spec: mustStruct(t, map[string]any{
				"group":   "example",
				"version": "v1",
				"kind":    "Thing",
			}),
		},
	}

	if err := installer.Apply(context.Background(), initial); err != nil {
		t.Fatalf("initial Apply failed: %v", err)
	}

	if err := installer.Apply(context.Background(), initial); err != nil {
		t.Fatalf("second Apply with identical record failed: %v", err)
	}
}

func TestInstallerApplyRejectsImmutableChange(t *testing.T) {
	recordsClient, cleanup := setupExtensionTestServer(t)
	defer cleanup()

	installer := &Installer{Records: recordsClient}

	initial := []*pb.Record{
		{
			TypeMeta: &pb.TypeMeta{
				Group:   "core",
				Version: "v1",
				Kind:    "MetaRecord",
			},
			ObjectMeta: &pb.ObjectMeta{
				Name:       "Thing.example",
				Tradespace: "default",
			},
			Spec: mustStruct(t, map[string]any{
				"group":   "example",
				"version": "v1",
				"kind":    "Thing",
			}),
		},
	}

	if err := installer.Apply(context.Background(), initial); err != nil {
		t.Fatalf("initial Apply failed: %v", err)
	}

	changed := []*pb.Record{
		{
			TypeMeta: initial[0].TypeMeta,
			ObjectMeta: &pb.ObjectMeta{
				Name:       "Thing.example",
				Tradespace: "default",
			},
			Spec: mustStruct(t, map[string]any{
				"group":   "example",
				"version": "v1",
				"kind":    "Thing",
				"scope":   "global",
			}),
		},
	}

	err := installer.Apply(context.Background(), changed)
	if !errors.Is(err, ErrImmutableRecord) {
		t.Fatalf("expected ErrImmutableRecord, got %v", err)
	}

	resp, err := recordsClient.Get(context.Background(), &pb.GetRequest{
		Type:       "core/v1/MetaRecord",
		Tradespace: "default",
		Name:       "Thing.example",
	})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if _, ok := resp.Record.Spec.Fields["scope"]; ok {
		t.Fatalf("expected original schema to remain unchanged, got scope=%q", resp.Record.Spec.Fields["scope"].GetStringValue())
	}
}

func mustStruct(t *testing.T, data map[string]any) *structpb.Struct {
	t.Helper()

	s, err := structpb.NewStruct(data)
	if err != nil {
		t.Fatalf("failed to build struct: %v", err)
	}
	return s
}
