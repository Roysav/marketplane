package main

import (
	"context"
	"flag"
	"log/slog"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/roysav/marketplane/api/gen"
	"github.com/roysav/marketplane/pkg/extension"
)

func main() {
	var (
		serverAddr = flag.String("server", "localhost:50051", "API server address")
		path       = flag.String("f", "", "Schema manifest file or directory")
		debug      = flag.Bool("debug", false, "Enable debug logging")
	)
	flag.Parse()

	if *path == "" {
		slog.Error("schema path is required", "flag", "-f")
		os.Exit(1)
	}

	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	records, err := extension.LoadRecords(*path)
	if err != nil {
		logger.Error("failed to load schema records", "path", *path, "error", err)
		os.Exit(1)
	}

	conn, err := grpc.NewClient(*serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Error("failed to connect to server", "addr", *serverAddr, "error", err)
		os.Exit(1)
	}
	defer conn.Close()

	installer := &extension.Installer{
		Records: pb.NewRecordServiceClient(conn),
	}
	if err := installer.Apply(context.Background(), records); err != nil {
		logger.Error("failed to apply schema records", "error", err)
		os.Exit(1)
	}

	logger.Info("schema records applied", "count", len(records), "path", *path)
}
