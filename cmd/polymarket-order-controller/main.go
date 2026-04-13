package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	pb "github.com/roysav/marketplane/api/gen"
	"github.com/roysav/marketplane/pkg/controller"
	"github.com/roysav/marketplane/pkg/polymarket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	var (
		serverAddr     = flag.String("server", "localhost:50051", "gRPC server address")
		resyncInterval = flag.Duration("resync", 30*time.Second, "Interval between full resyncs")
		debug          = flag.Bool("debug", false, "Enable debug logging")
	)
	flag.Parse()

	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	polyCfg := polymarket.Config{
		Host:          requireEnv("POLY_HOST"),
		ChainID:       parseChainID(os.Getenv("POLY_CHAIN_ID")),
		Address:       requireEnv("POLY_ADDRESS"),
		MakerAddress:  os.Getenv("POLY_MAKER_ADDRESS"),
		PrivateKey:    requireEnv("POLY_PRIVATE_KEY"),
		APIKey:        requireEnv("POLY_API_KEY"),
		APISecret:     requireEnv("POLY_API_SECRET"),
		APIPassphrase: requireEnv("POLY_API_PASSPHRASE"),
		SignatureType: parseSignatureType(os.Getenv("POLY_SIGNATURE_TYPE")),
		Funder:        os.Getenv("POLY_FUNDER"),
	}

	polyClient, err := polymarket.NewClient(polyCfg)
	if err != nil {
		panic(fmt.Sprintf("failed to create polymarket client: %v", err))
	}

	conn, err := grpc.NewClient(*serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(fmt.Sprintf("failed to connect: %v", err))
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		logger.Info("shutting down...")
		cancel()
	}()

	ctrl := controller.NewPolymarketOrderController(controller.PolymarketOrderControllerConfig{
		Records:        pb.NewRecordServiceClient(conn),
		Poly:           polyClient,
		Logger:         logger,
		ResyncInterval: *resyncInterval,
	})

	logger.Info("polymarket order controller started", "server", *serverAddr, "resync", *resyncInterval)
	if err := ctrl.Run(ctx); err != nil && ctx.Err() == nil {
		panic(fmt.Sprintf("controller failed: %v", err))
	}
}

func requireEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		panic(fmt.Sprintf("required env var %s is not set", key))
	}
	return v
}

func parseChainID(s string) int64 {
	if s == "" {
		return 137 // Polygon mainnet
	}
	id, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		panic(fmt.Sprintf("invalid POLY_CHAIN_ID: %s", s))
	}
	return id
}

func parseSignatureType(s string) int {
	if s == "" {
		return 1 // POLY_PROXY default
	}
	t, err := strconv.Atoi(s)
	if err != nil {
		panic(fmt.Sprintf("invalid POLY_SIGNATURE_TYPE: %s", s))
	}
	return t
}
