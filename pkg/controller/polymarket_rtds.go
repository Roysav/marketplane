package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/roysav/marketplane/api/gen"
)

const (
	CryptoSubscriptionTypeStr         = "polymarket/v1/CryptoSubscription"
	PhaseActive                       = "Active"
	PhaseError                        = "Error"
	PhasePaused                       = "Paused"
	DefaultPolymarketRTDSEndpoint     = "wss://ws-live-data.polymarket.com"
	DefaultPolymarketPingInterval     = 5 * time.Second
	DefaultPolymarketReconnectDelay   = 3 * time.Second
	DefaultPolymarketStatusWriteDelay = 5 * time.Second
	polymarketDefaultSyncInterval     = 30 * time.Second
	polymarketDefaultTradespace       = "default"
	polymarketVenueBinance            = "binance"
	polymarketVenueChainlink          = "chainlink"
	polymarketTopicBinance            = "crypto_prices"
	polymarketTopicChainlink          = "crypto_prices_chainlink"
)

type websocketDialer interface {
	DialContext(ctx context.Context, urlStr string, requestHeader http.Header) (*websocket.Conn, *http.Response, error)
}

type cryptoSubscription struct {
	Name       string
	Venue      string
	Symbol     string
	Enabled    bool
	StreamName string
}

type polymarketRTDSMessage struct {
	Topic     string                     `json:"topic"`
	Type      string                     `json:"type"`
	Timestamp int64                      `json:"timestamp"`
	Payload   polymarketRTDSPricePayload `json:"payload"`
}

type polymarketRTDSPricePayload struct {
	Symbol    string  `json:"symbol"`
	Timestamp int64   `json:"timestamp"`
	Value     float64 `json:"value"`
}

type polymarketSubscribeRequest struct {
	Action        string                            `json:"action"`
	Subscriptions []polymarketSubscribeSubscription `json:"subscriptions"`
}

type polymarketSubscribeSubscription struct {
	Topic   string `json:"topic"`
	Type    string `json:"type"`
	Filters string `json:"filters,omitempty"`
}

// PolymarketRTDSController ingests RTDS crypto prices into Marketplane streams.
type PolymarketRTDSController struct {
	records             pb.RecordServiceClient
	streams             pb.StreamServiceClient
	logger              *slog.Logger
	endpoint            string
	pingInterval        time.Duration
	reconnectDelay      time.Duration
	statusWriteInterval time.Duration
	syncInterval        time.Duration
	dialer              websocketDialer
	now                 func() time.Time
	fingerprints        map[string]string
	desired             map[string]cryptoSubscription
	workers             map[string]*polymarketVenueWorker
	statusWriteMu       sync.Mutex
	lastStatusWrite     map[string]time.Time
}

// NewPolymarketRTDSController creates a new Polymarket RTDS controller.
func NewPolymarketRTDSController(records pb.RecordServiceClient, streams pb.StreamServiceClient, logger *slog.Logger) *PolymarketRTDSController {
	if logger == nil {
		logger = slog.Default()
	}

	return &PolymarketRTDSController{
		records:             records,
		streams:             streams,
		logger:              logger.With("component", "polymarket-rtds-controller"),
		endpoint:            DefaultPolymarketRTDSEndpoint,
		pingInterval:        DefaultPolymarketPingInterval,
		reconnectDelay:      DefaultPolymarketReconnectDelay,
		statusWriteInterval: DefaultPolymarketStatusWriteDelay,
		syncInterval:        polymarketDefaultSyncInterval,
		dialer:              websocket.DefaultDialer,
		now:                 time.Now,
		fingerprints:        make(map[string]string),
		desired:             make(map[string]cryptoSubscription),
		workers:             make(map[string]*polymarketVenueWorker),
		lastStatusWrite:     make(map[string]time.Time),
	}
}

// SetEndpoint overrides the RTDS websocket endpoint.
func (c *PolymarketRTDSController) SetEndpoint(endpoint string) {
	if endpoint != "" {
		c.endpoint = endpoint
	}
}

// SetPingInterval overrides the RTDS keepalive interval.
func (c *PolymarketRTDSController) SetPingInterval(d time.Duration) {
	if d > 0 {
		c.pingInterval = d
	}
}

// SetReconnectDelay overrides the reconnect backoff.
func (c *PolymarketRTDSController) SetReconnectDelay(d time.Duration) {
	if d > 0 {
		c.reconnectDelay = d
	}
}

// SetStatusWriteInterval overrides the minimum spacing between lastMessageAt writes.
func (c *PolymarketRTDSController) SetStatusWriteInterval(d time.Duration) {
	if d >= 0 {
		c.statusWriteInterval = d
	}
}

// SetSyncInterval overrides the periodic resync interval.
func (c *PolymarketRTDSController) SetSyncInterval(d time.Duration) {
	if d > 0 {
		c.syncInterval = d
	}
}

// Run starts the Polymarket RTDS ingestion loop.
func (c *PolymarketRTDSController) Run(ctx context.Context) error {
	c.logger.Info("polymarket RTDS controller started", "endpoint", c.endpoint)

	if err := c.loadInitialState(ctx); err != nil {
		return err
	}

	for _, venue := range supportedPolymarketVenues() {
		worker := &polymarketVenueWorker{
			venue:      venue,
			controller: c,
			updates:    make(chan []cryptoSubscription, 1),
		}
		c.workers[venue] = worker
		go worker.run(ctx)
	}

	c.pushVenueUpdates()

	stream, err := c.records.Watch(ctx, &pb.WatchRequest{Type: CryptoSubscriptionTypeStr})
	if err != nil {
		return err
	}
	if err := c.syncState(ctx, false); err != nil {
		c.logger.Error("post-watch resync failed", "error", err)
	}

	ticker := time.NewTicker(c.syncInterval)
	defer ticker.Stop()

	eventCh := make(chan *pb.WatchEvent)
	errCh := make(chan error, 1)

	go func() {
		for {
			event, recvErr := stream.Recv()
			if recvErr != nil {
				errCh <- recvErr
				return
			}

			select {
			case eventCh <- event:
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("polymarket RTDS controller stopped")
			return nil
		case recvErr := <-errCh:
			if errors.Is(recvErr, io.EOF) || errors.Is(recvErr, context.Canceled) {
				c.logger.Info("polymarket RTDS controller stopped")
				return nil
			}
			return recvErr
		case <-ticker.C:
			if err := c.syncState(ctx, false); err != nil {
				c.logger.Error("periodic resync failed", "error", err)
			}
		case event := <-eventCh:
			if event == nil || event.Record == nil {
				continue
			}
			if err := c.handleWatchEvent(ctx, event); err != nil {
				c.logger.Error("failed to process subscription event", "action", event.Action, "name", event.Record.ObjectMeta.GetName(), "error", err)
			}
		}
	}
}

func (c *PolymarketRTDSController) loadInitialState(ctx context.Context) error {
	return c.syncState(ctx, true)
}

func (c *PolymarketRTDSController) syncState(ctx context.Context, startup bool) error {
	resp, err := c.records.List(ctx, &pb.ListRequest{Type: CryptoSubscriptionTypeStr})
	if err != nil {
		return err
	}

	nextDesired := make(map[string]cryptoSubscription)
	nextFingerprints := make(map[string]string, len(resp.Records))
	var startupErrors []string

	for _, rec := range resp.Records {
		if rec == nil {
			continue
		}

		if rec.ObjectMeta != nil && rec.ObjectMeta.GetName() != "" {
			nextFingerprints[rec.ObjectMeta.GetName()] = fingerprintSubscriptionRecord(rec)
		}

		sub, prepErr := c.prepareSubscription(ctx, rec, startup)
		if prepErr != nil {
			startupErrors = append(startupErrors, prepErr.Error())
			continue
		}
		if sub != nil {
			nextDesired[sub.Name] = *sub
		}
	}

	c.statusWriteMu.Lock()
	for name := range c.lastStatusWrite {
		if _, ok := nextDesired[name]; !ok {
			delete(c.lastStatusWrite, name)
		}
	}
	c.statusWriteMu.Unlock()

	if startup && len(startupErrors) > 0 {
		return fmt.Errorf("startup validation failed: %s", strings.Join(startupErrors, "; "))
	}

	changed := !equalSubscriptionMaps(c.desired, nextDesired) || !equalStringMaps(c.fingerprints, nextFingerprints)
	c.desired = nextDesired
	c.fingerprints = nextFingerprints
	if changed {
		c.pushVenueUpdates()
	}

	return nil
}

func (c *PolymarketRTDSController) handleWatchEvent(ctx context.Context, event *pb.WatchEvent) error {
	name := event.Record.ObjectMeta.GetName()
	if name == "" {
		return nil
	}

	if event.Action == "deleted" {
		if _, exists := c.desired[name]; exists {
			delete(c.desired, name)
			c.pushVenueUpdates()
		}
		delete(c.fingerprints, name)
		c.statusWriteMu.Lock()
		delete(c.lastStatusWrite, name)
		c.statusWriteMu.Unlock()
		return nil
	}

	fingerprint := fingerprintSubscriptionRecord(event.Record)
	if previous, ok := c.fingerprints[name]; ok && previous == fingerprint {
		return nil
	}

	sub, err := c.prepareSubscription(ctx, event.Record, false)
	if err != nil {
		return err
	}

	c.fingerprints[name] = fingerprint
	if sub == nil {
		delete(c.desired, name)
		c.statusWriteMu.Lock()
		delete(c.lastStatusWrite, name)
		c.statusWriteMu.Unlock()
	} else {
		c.desired[name] = *sub
	}

	c.pushVenueUpdates()
	return nil
}

func (c *PolymarketRTDSController) prepareSubscription(ctx context.Context, rec *pb.Record, startup bool) (*cryptoSubscription, error) {
	recordName := ""
	if rec != nil && rec.ObjectMeta != nil {
		recordName = rec.ObjectMeta.GetName()
	}

	sub, err := parseCryptoSubscription(rec)
	if err != nil {
		if statusErr := c.updateSubscriptionStatus(ctx, recordName, subscriptionStatusMutation{
			Phase:      PhaseError,
			StreamName: recordName,
			LastError:  stringPtr(err.Error()),
		}); statusErr != nil {
			c.logger.Warn("failed to persist invalid subscription status", "name", recordName, "error", statusErr)
		}
		return nil, nil
	}

	if !sub.Enabled {
		if err := c.updateSubscriptionStatus(ctx, sub.Name, subscriptionStatusMutation{
			Phase:      PhasePaused,
			StreamName: sub.StreamName,
			LastError:  stringPtr(""),
		}); err != nil {
			c.logger.Warn("failed to persist paused subscription status", "name", sub.Name, "error", err)
		}
		return nil, nil
	}

	if err := c.ensureStreamDefinition(ctx, sub.StreamName); err != nil {
		if statusErr := c.updateSubscriptionStatus(ctx, sub.Name, subscriptionStatusMutation{
			Phase:      PhaseError,
			StreamName: sub.StreamName,
			LastError:  stringPtr(err.Error()),
		}); statusErr != nil {
			c.logger.Warn("failed to persist missing-definition status", "name", sub.Name, "error", statusErr)
		}
		if startup {
			return nil, err
		}
		return nil, nil
	}

	if err := c.updateSubscriptionStatus(ctx, sub.Name, subscriptionStatusMutation{
		Phase:      PhasePending,
		StreamName: sub.StreamName,
		LastError:  stringPtr(""),
	}); err != nil {
		c.logger.Warn("failed to persist pending subscription status", "name", sub.Name, "error", err)
	}

	return sub, nil
}

func (c *PolymarketRTDSController) ensureStreamDefinition(ctx context.Context, name string) error {
	_, err := c.records.Get(ctx, &pb.GetRequest{
		Type:       "core/v1/StreamDefinition",
		Tradespace: polymarketDefaultTradespace,
		Name:       name,
	})
	if err == nil {
		return nil
	}
	if status.Code(err) == codes.NotFound {
		return fmt.Errorf("missing StreamDefinition %q", name)
	}
	return err
}

func (c *PolymarketRTDSController) pushVenueUpdates() {
	for _, venue := range supportedPolymarketVenues() {
		worker, ok := c.workers[venue]
		if !ok {
			continue
		}

		var subs []cryptoSubscription
		for _, sub := range c.desired {
			if sub.Venue == venue {
				subs = append(subs, sub)
			}
		}

		sort.Slice(subs, func(i, j int) bool {
			return subs[i].Name < subs[j].Name
		})
		worker.setSubscriptions(subs)
	}
}

func (c *PolymarketRTDSController) markSubscriptionsConnected(ctx context.Context, subs []cryptoSubscription, connectedAt time.Time) {
	for _, sub := range subs {
		if err := c.updateSubscriptionStatus(ctx, sub.Name, subscriptionStatusMutation{
			Phase:           PhaseActive,
			StreamName:      sub.StreamName,
			LastError:       stringPtr(""),
			LastConnectedAt: &connectedAt,
		}); err != nil {
			c.logger.Warn("failed to persist active status", "name", sub.Name, "error", err)
		}
	}
}

func (c *PolymarketRTDSController) markSubscriptionsError(ctx context.Context, subs []cryptoSubscription, err error) {
	if err == nil {
		return
	}

	for _, sub := range subs {
		if statusErr := c.updateSubscriptionStatus(ctx, sub.Name, subscriptionStatusMutation{
			Phase:      PhaseError,
			StreamName: sub.StreamName,
			LastError:  stringPtr(err.Error()),
		}); statusErr != nil {
			c.logger.Warn("failed to persist error status", "name", sub.Name, "error", statusErr)
		}
	}
}

func (c *PolymarketRTDSController) maybeMarkSubscriptionMessage(ctx context.Context, sub cryptoSubscription, msgTime time.Time) {
	c.statusWriteMu.Lock()
	lastWrite := c.lastStatusWrite[sub.Name]
	c.statusWriteMu.Unlock()

	if c.statusWriteInterval > 0 && !lastWrite.IsZero() && msgTime.Sub(lastWrite) < c.statusWriteInterval {
		return
	}

	if err := c.updateSubscriptionStatus(ctx, sub.Name, subscriptionStatusMutation{
		Phase:         PhaseActive,
		StreamName:    sub.StreamName,
		LastError:     stringPtr(""),
		LastMessageAt: &msgTime,
	}); err != nil {
		c.logger.Warn("failed to persist last message time", "name", sub.Name, "error", err)
		return
	}

	c.statusWriteMu.Lock()
	c.lastStatusWrite[sub.Name] = msgTime
	c.statusWriteMu.Unlock()
}

func (c *PolymarketRTDSController) appendPrice(ctx context.Context, sub cryptoSubscription, msg polymarketRTDSMessage) error {
	data, err := structpb.NewStruct(map[string]any{
		"venue":                sub.Venue,
		"symbol":               msg.Payload.Symbol,
		"value":                msg.Payload.Value,
		"source_timestamp_ms":  float64(msg.Payload.Timestamp),
		"message_timestamp_ms": float64(msg.Timestamp),
	})
	if err != nil {
		return err
	}

	_, err = c.streams.Append(ctx, &pb.AppendRequest{
		Key: &pb.StreamKey{
			Name: sub.StreamName,
		},
		Timestamp: timestamppb.New(time.UnixMilli(msg.Timestamp)),
		Data:      data,
	})
	if err != nil {
		return err
	}

	c.maybeMarkSubscriptionMessage(ctx, sub, time.UnixMilli(msg.Timestamp).UTC())
	return nil
}

type subscriptionStatusMutation struct {
	Phase           string
	StreamName      string
	LastError       *string
	LastConnectedAt *time.Time
	LastMessageAt   *time.Time
}

func (c *PolymarketRTDSController) updateSubscriptionStatus(ctx context.Context, name string, mutation subscriptionStatusMutation) error {
	if name == "" {
		return nil
	}

	resp, err := c.records.Get(ctx, &pb.GetRequest{
		Type:       CryptoSubscriptionTypeStr,
		Tradespace: polymarketDefaultTradespace,
		Name:       name,
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil
		}
		return err
	}

	rec := resp.Record
	statusMap := make(map[string]any)
	if rec.Status != nil {
		for key, value := range rec.Status.AsMap() {
			statusMap[key] = value
		}
	}

	if mutation.Phase != "" {
		statusMap["phase"] = mutation.Phase
	}
	if mutation.StreamName != "" {
		statusMap["streamName"] = mutation.StreamName
	}
	if mutation.LastError != nil {
		statusMap["lastError"] = *mutation.LastError
	}
	if mutation.LastConnectedAt != nil {
		statusMap["lastConnectedAt"] = mutation.LastConnectedAt.UTC().Format(time.RFC3339Nano)
	}
	if mutation.LastMessageAt != nil {
		statusMap["lastMessageAt"] = mutation.LastMessageAt.UTC().Format(time.RFC3339Nano)
	}

	statusStruct, err := structpb.NewStruct(statusMap)
	if err != nil {
		return err
	}

	rec.Status = statusStruct
	_, err = c.records.Update(ctx, &pb.UpdateRequest{Record: rec})
	return err
}

type polymarketVenueWorker struct {
	venue      string
	controller *PolymarketRTDSController
	updates    chan []cryptoSubscription
}

func (w *polymarketVenueWorker) setSubscriptions(subs []cryptoSubscription) {
	select {
	case w.updates <- subs:
	default:
		select {
		case <-w.updates:
		default:
		}
		w.updates <- subs
	}
}

func (w *polymarketVenueWorker) run(ctx context.Context) {
	var current []cryptoSubscription

	for {
		current = w.drainUpdates(current)
		if len(current) == 0 {
			select {
			case <-ctx.Done():
				return
			case current = <-w.updates:
				continue
			}
		}

		conn, _, err := w.controller.dialer.DialContext(ctx, w.controller.endpoint, nil)
		if err != nil {
			w.controller.markSubscriptionsError(ctx, current, fmt.Errorf("failed to connect to RTDS: %w", err))
			if !w.waitForRetry(ctx, &current) {
				return
			}
			continue
		}

		if err := w.subscribe(conn, current); err != nil {
			_ = conn.Close()
			w.controller.markSubscriptionsError(ctx, current, fmt.Errorf("failed to subscribe to RTDS: %w", err))
			if !w.waitForRetry(ctx, &current) {
				return
			}
			continue
		}

		w.controller.markSubscriptionsConnected(ctx, current, w.controller.now().UTC())

		readErrCh := make(chan error, 1)
		go func(snapshot map[string]cryptoSubscription) {
			readErrCh <- w.readLoop(ctx, conn, snapshot)
		}(subscriptionsByStreamName(current))

		pingTicker := time.NewTicker(w.controller.pingInterval)
		retryAfterFailure := false

	loop:
		for {
			select {
			case <-ctx.Done():
				pingTicker.Stop()
				_ = conn.Close()
				return
			case updated := <-w.updates:
				current = updated
				pingTicker.Stop()
				_ = conn.Close()
				break loop
			case <-pingTicker.C:
				if pingErr := conn.WriteMessage(websocket.TextMessage, []byte("PING")); pingErr != nil {
					w.controller.markSubscriptionsError(ctx, current, fmt.Errorf("failed to send RTDS ping: %w", pingErr))
					retryAfterFailure = true
					pingTicker.Stop()
					_ = conn.Close()
					break loop
				}
			case readErr := <-readErrCh:
				if readErr != nil && !errors.Is(readErr, context.Canceled) {
					w.controller.markSubscriptionsError(ctx, current, fmt.Errorf("RTDS stream closed: %w", readErr))
					retryAfterFailure = true
				}
				pingTicker.Stop()
				_ = conn.Close()
				break loop
			}
		}

		if retryAfterFailure {
			if !w.waitForRetry(ctx, &current) {
				return
			}
		}
	}
}

func (w *polymarketVenueWorker) waitForRetry(ctx context.Context, current *[]cryptoSubscription) bool {
	timer := time.NewTimer(w.controller.reconnectDelay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case updated := <-w.updates:
		*current = updated
		return true
	case <-timer.C:
		*current = w.drainUpdates(*current)
		return true
	}
}

func (w *polymarketVenueWorker) drainUpdates(current []cryptoSubscription) []cryptoSubscription {
	for {
		select {
		case updated := <-w.updates:
			current = updated
		default:
			return current
		}
	}
}

func (w *polymarketVenueWorker) subscribe(conn *websocket.Conn, subs []cryptoSubscription) error {
	payload, err := buildSubscribePayload(w.venue, subs)
	if err != nil {
		return err
	}
	w.controller.logger.Debug("subscribing to RTDS", "venue", w.venue, "subscriptions", len(subs), "payload", payload)
	return conn.WriteJSON(payload)
}

func (w *polymarketVenueWorker) readLoop(ctx context.Context, conn *websocket.Conn, subs map[string]cryptoSubscription) error {
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		msg, err := decodePolymarketRTDSMessage(data)
		if err != nil {
			w.controller.logger.Debug("ignoring RTDS frame", "venue", w.venue, "error", err)
			continue
		}
		if msg.Type != "update" {
			continue
		}

		streamName, err := streamNameForVenue(w.venue, msg.Payload.Symbol)
		if err != nil {
			w.controller.logger.Debug("ignoring RTDS symbol", "venue", w.venue, "symbol", msg.Payload.Symbol, "error", err)
			continue
		}

		sub, ok := subs[streamName]
		if !ok {
			continue
		}

		if err := w.controller.appendPrice(ctx, sub, msg); err != nil {
			w.controller.logger.Error("failed to append RTDS message", "stream", sub.StreamName, "error", err)
			if statusErr := w.controller.updateSubscriptionStatus(ctx, sub.Name, subscriptionStatusMutation{
				Phase:      PhaseError,
				StreamName: sub.StreamName,
				LastError:  stringPtr(err.Error()),
			}); statusErr != nil {
				w.controller.logger.Warn("failed to persist append error", "name", sub.Name, "error", statusErr)
			}
		}
	}
}

func parseCryptoSubscription(rec *pb.Record) (*cryptoSubscription, error) {
	if rec == nil || rec.ObjectMeta == nil {
		return nil, errors.New("record metadata is required")
	}

	name := rec.ObjectMeta.GetName()
	if name == "" {
		return nil, errors.New("subscription name is required")
	}

	spec := map[string]any{}
	if rec.Spec != nil {
		spec = rec.Spec.AsMap()
	}

	venue, ok := spec["venue"].(string)
	if !ok || strings.TrimSpace(venue) == "" {
		return nil, errors.New("spec.venue is required")
	}

	symbol, ok := spec["symbol"].(string)
	if !ok || strings.TrimSpace(symbol) == "" {
		return nil, errors.New("spec.symbol is required")
	}

	enabled := true
	if rawEnabled, ok := spec["enabled"]; ok {
		boolEnabled, ok := rawEnabled.(bool)
		if !ok {
			return nil, errors.New("spec.enabled must be a boolean")
		}
		enabled = boolEnabled
	}

	streamName, err := streamNameForVenue(venue, symbol)
	if err != nil {
		return nil, err
	}
	if name != streamName {
		return nil, fmt.Errorf("record name %q must equal stream name %q", name, streamName)
	}

	return &cryptoSubscription{
		Name:       name,
		Venue:      normalizeVenue(venue),
		Symbol:     normalizeSymbol(venue, symbol),
		Enabled:    enabled,
		StreamName: streamName,
	}, nil
}

func streamNameForVenue(venue, symbol string) (string, error) {
	normalizedVenue := normalizeVenue(venue)
	normalizedSymbol := normalizeSymbol(normalizedVenue, symbol)

	switch normalizedVenue {
	case polymarketVenueBinance:
		if strings.Contains(normalizedSymbol, "/") || normalizedSymbol == "" {
			return "", fmt.Errorf("invalid Binance symbol %q", symbol)
		}
	case polymarketVenueChainlink:
		parts := strings.Split(normalizedSymbol, "/")
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return "", fmt.Errorf("invalid Chainlink symbol %q", symbol)
		}
	default:
		return "", fmt.Errorf("unsupported venue %q", venue)
	}

	streamSymbol := strings.ReplaceAll(normalizedSymbol, "/", "_")
	return fmt.Sprintf("polymarket.crypto.%s.%s", normalizedVenue, streamSymbol), nil
}

func normalizeVenue(venue string) string {
	return strings.ToLower(strings.TrimSpace(venue))
}

func normalizeSymbol(venue, symbol string) string {
	normalized := strings.ToLower(strings.TrimSpace(symbol))
	if normalizeVenue(venue) == polymarketVenueChainlink {
		return normalized
	}
	return strings.ReplaceAll(normalized, " ", "")
}

func buildSubscribePayload(venue string, subs []cryptoSubscription) (*polymarketSubscribeRequest, error) {
	if len(subs) == 0 {
		return nil, errors.New("at least one subscription is required")
	}

	switch normalizeVenue(venue) {
	case polymarketVenueBinance:
		sort.Slice(subs, func(i, j int) bool {
			return subs[i].Symbol < subs[j].Symbol
		})
		req := &polymarketSubscribeRequest{
			Action:        "subscribe",
			Subscriptions: make([]polymarketSubscribeSubscription, 0, len(subs)),
		}
		for _, sub := range subs {
			// Live RTDS expects Binance filters as JSON objects, even though the
			// docs currently show a comma-separated string example.
			filterBytes, err := json.Marshal(map[string]string{"symbol": sub.Symbol})
			if err != nil {
				return nil, err
			}
			req.Subscriptions = append(req.Subscriptions, polymarketSubscribeSubscription{
				Topic:   polymarketTopicBinance,
				Type:    "update",
				Filters: string(filterBytes),
			})
		}
		return req, nil
	case polymarketVenueChainlink:
		sort.Slice(subs, func(i, j int) bool {
			return subs[i].Symbol < subs[j].Symbol
		})
		req := &polymarketSubscribeRequest{
			Action:        "subscribe",
			Subscriptions: make([]polymarketSubscribeSubscription, 0, len(subs)),
		}
		for _, sub := range subs {
			filterBytes, err := json.Marshal(map[string]string{"symbol": sub.Symbol})
			if err != nil {
				return nil, err
			}
			req.Subscriptions = append(req.Subscriptions, polymarketSubscribeSubscription{
				Topic:   polymarketTopicChainlink,
				Type:    "*",
				Filters: string(filterBytes),
			})
		}
		return req, nil
	default:
		return nil, fmt.Errorf("unsupported venue %q", venue)
	}
}

func decodePolymarketRTDSMessage(data []byte) (polymarketRTDSMessage, error) {
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" || trimmed == "PONG" {
		return polymarketRTDSMessage{}, errors.New("not a RTDS JSON payload")
	}

	var msg polymarketRTDSMessage
	if err := json.Unmarshal([]byte(trimmed), &msg); err != nil {
		return polymarketRTDSMessage{}, err
	}
	if msg.Payload.Symbol == "" {
		return polymarketRTDSMessage{}, errors.New("message payload.symbol is required")
	}
	return msg, nil
}

func subscriptionsByStreamName(subs []cryptoSubscription) map[string]cryptoSubscription {
	result := make(map[string]cryptoSubscription, len(subs))
	for _, sub := range subs {
		result[sub.StreamName] = sub
	}
	return result
}

func fingerprintSubscriptionRecord(rec *pb.Record) string {
	if rec == nil || rec.ObjectMeta == nil {
		return ""
	}

	payload, err := json.Marshal(map[string]any{
		"name": rec.ObjectMeta.GetName(),
		"spec": func() map[string]any {
			if rec.Spec == nil {
				return nil
			}
			return rec.Spec.AsMap()
		}(),
	})
	if err != nil {
		return rec.ObjectMeta.GetName()
	}
	return string(payload)
}

func supportedPolymarketVenues() []string {
	return []string{polymarketVenueBinance, polymarketVenueChainlink}
}

func equalSubscriptionMaps(a, b map[string]cryptoSubscription) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		if other, ok := b[key]; !ok || other != value {
			return false
		}
	}
	return true
}

func equalStringMaps(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		if other, ok := b[key]; !ok || other != value {
			return false
		}
	}
	return true
}

func stringPtr(s string) *string {
	return &s
}
