package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/roysav/marketplane/pkg/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/roysav/marketplane/api/gen"
	"github.com/roysav/marketplane/pkg/controller"
)

func main() {
	var (
		serverAddr    = flag.String("server", "localhost:50051", "API server address")
		debug         = flag.Bool("debug", false, "Enable debug logging")
		tlsCert       = flag.String("cert", "", "Path to PEM-encoded client certificate")
		tlsKey        = flag.String("key", "", "Path to PEM-encoded client private key")
		tlsCA         = flag.String("ca", "", "Path to PEM-encoded CA certificate")
		tlsInsecure   = flag.Bool("insecure", false, "Disable TLS and connect in plaintext mode")
		tlsServerName = flag.String("server-name", "", "Override TLS server name")
	)
	flag.Parse()

	// Setup logger
	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Connect to API server
	logger.Info("connecting to API server", "addr", *serverAddr)
	creds, err := controllerCredentials(*serverAddr, *tlsCert, *tlsKey, *tlsCA, *tlsServerName, *tlsInsecure)
	if err != nil {
		logger.Error("failed to load client credentials", "error", err)
		os.Exit(1)
	}
	conn, err := grpc.NewClient(*serverAddr, grpc.WithTransportCredentials(creds))
	if err != nil {
		logger.Error("failed to connect to server", "error", err)
		os.Exit(1)
	}
	defer conn.Close()

	// Create gRPC clients
	recordsClient := pb.NewRecordServiceClient(conn)
	ledgerClient := pb.NewLedgerServiceClient(conn)

	// Create and run the allocation controller
	ctrl := controller.NewAllocationController(recordsClient, ledgerClient, logger)

	// Handle shutdown
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		logger.Info("shutting down...")
		cancel()
	}()

	logger.Info("starting allocation controller")
	if err := ctrl.Run(ctx); err != nil && err != context.Canceled {
		logger.Error("controller error", "error", err)
		os.Exit(1)
	}
}

func controllerCredentials(serverAddr, certFile, keyFile, caFile, serverName string, allowInsecure bool) (credentials.TransportCredentials, error) {
	switch {
	case certFile != "" && keyFile != "" && caFile != "":
		if serverName == "" {
			serverName = inferServerName(serverAddr)
		}
		return auth.ClientCredentials(certFile, keyFile, caFile, serverName)
	case allowInsecure:
		return insecure.NewCredentials(), nil
	default:
		return nil, fmt.Errorf("controller TLS requires -cert, -key, and -ca, or use -insecure")
	}
}

func inferServerName(serverAddr string) string {
	host := serverAddr
	if idx := strings.LastIndex(serverAddr, ":"); idx >= 0 {
		host = serverAddr[:idx]
	}
	host = strings.TrimSpace(host)
	if host == "" {
		return "localhost"
	}
	return host
}
