package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/roysav/marketplane/api/gen"
	"github.com/roysav/marketplane/pkg/controller"
)

func main() {
	var (
		serverAddr       = flag.String("server", "localhost:50051", "API server address")
		enableAllocation = flag.Bool("allocation", true, "Run the allocation controller")
		enablePolymarket = flag.Bool("polymarket-rtds", true, "Run the Polymarket RTDS controller")
		rtdsEndpoint     = flag.String("rtds-endpoint", controller.DefaultPolymarketRTDSEndpoint, "Polymarket RTDS websocket endpoint")
		pingInterval     = flag.Duration("ping-interval", controller.DefaultPolymarketPingInterval, "RTDS ping interval")
		reconnectDelay   = flag.Duration("reconnect-delay", controller.DefaultPolymarketReconnectDelay, "RTDS reconnect delay")
		debug            = flag.Bool("debug", false, "Enable debug logging")
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
	conn, err := grpc.NewClient(*serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Error("failed to connect to server", "error", err)
		os.Exit(1)
	}
	defer conn.Close()

	// Create gRPC clients
	recordsClient := pb.NewRecordServiceClient(conn)
	ledgerClient := pb.NewLedgerServiceClient(conn)
	streamsClient := pb.NewStreamServiceClient(conn)

	// Handle shutdown
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		logger.Info("shutting down...")
		cancel()
	}()

	started := 0
	errCh := make(chan error, 2)

	if *enableAllocation {
		ctrl := controller.NewAllocationController(recordsClient, ledgerClient, logger)
		started++
		logger.Info("starting allocation controller")
		go runController(ctx, errCh, "allocation", ctrl.Run)
	}

	if *enablePolymarket {
		ctrl := controller.NewPolymarketRTDSController(recordsClient, streamsClient, logger)
		ctrl.SetEndpoint(*rtdsEndpoint)
		ctrl.SetPingInterval(*pingInterval)
		ctrl.SetReconnectDelay(*reconnectDelay)

		started++
		logger.Info("starting Polymarket RTDS controller", "endpoint", *rtdsEndpoint)
		go runController(ctx, errCh, "polymarket-rtds", ctrl.Run)
	}

	if started == 0 {
		logger.Error("no controllers enabled")
		os.Exit(1)
	}

	for i := 0; i < started; i++ {
		err := <-errCh
		if err != nil && err != context.Canceled {
			logger.Error("controller manager error", "error", err)
			cancel()
			os.Exit(1)
		}
	}
}

func runController(ctx context.Context, errCh chan<- error, name string, run func(context.Context) error) {
	err := run(ctx)
	if err != nil && err != context.Canceled {
		err = fmt.Errorf("%s: %w", name, err)
	}
	errCh <- err
}
