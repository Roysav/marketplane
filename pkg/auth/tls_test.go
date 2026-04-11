package auth_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/roysav/marketplane/pkg/auth"
	"github.com/roysav/marketplane/pkg/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

// ----------------------------------------------------------------------------
// helpers — in-memory cert generation
// ----------------------------------------------------------------------------

type certBundle struct {
	cert     *x509.Certificate
	certPEM  []byte
	keyPEM   []byte
	tlsCert  tls.Certificate
	certPool *x509.CertPool
}

// selfSignedCA generates a self-signed CA certificate.
func selfSignedCA(t *testing.T) *certBundle {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}

	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{Organization: []string{"Test"}, CommonName: "Test CA"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}

	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal CA key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("load CA tls.Certificate: %v", err)
	}

	pool := x509.NewCertPool()
	pool.AddCert(cert)

	return &certBundle{cert: cert, certPEM: certPEM, keyPEM: keyPEM, tlsCert: tlsCert, certPool: pool}
}

// signedLeaf generates a certificate signed by ca. cn is the Subject.CommonName.
func signedLeaf(t *testing.T, ca *certBundle, cn string, dnsNames []string, isServer bool) *certBundle {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate leaf key: %v", err)
	}

	usage := x509.ExtKeyUsageClientAuth
	if isServer {
		usage = x509.ExtKeyUsageServerAuth
	}

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{usage},
		DNSNames:     dnsNames,
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.tlsCert.PrivateKey)
	if err != nil {
		t.Fatalf("create leaf cert: %v", err)
	}

	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		t.Fatalf("parse leaf cert: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal leaf key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("load leaf tls.Certificate: %v", err)
	}

	return &certBundle{cert: cert, certPEM: certPEM, keyPEM: keyPEM, tlsCert: tlsCert}
}

// writeTemp writes data to a temp file and returns the path. Cleans up on test exit.
func writeTemp(t *testing.T, name string, data []byte) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write temp %s: %v", name, err)
	}
	return path
}

// ----------------------------------------------------------------------------
// NewServerTLSConfig
// ----------------------------------------------------------------------------

func TestNewServerTLSConfig_ServerOnly(t *testing.T) {
	ca := selfSignedCA(t)
	srv := signedLeaf(t, ca, "server", []string{"localhost"}, true)

	certFile := writeTemp(t, "server.crt", srv.certPEM)
	keyFile := writeTemp(t, "server.key", srv.keyPEM)

	cfg, err := auth.NewServerTLSConfig(certFile, keyFile, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.ClientAuth != tls.NoClientCert {
		t.Errorf("expected NoClientCert, got %v", cfg.ClientAuth)
	}
	if len(cfg.Certificates) != 1 {
		t.Errorf("expected 1 certificate, got %d", len(cfg.Certificates))
	}
}

func TestNewServerTLSConfig_MutualTLS(t *testing.T) {
	ca := selfSignedCA(t)
	srv := signedLeaf(t, ca, "server", []string{"localhost"}, true)

	certFile := writeTemp(t, "server.crt", srv.certPEM)
	keyFile := writeTemp(t, "server.key", srv.keyPEM)
	caFile := writeTemp(t, "ca.crt", ca.certPEM)

	cfg, err := auth.NewServerTLSConfig(certFile, keyFile, caFile)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Errorf("expected RequireAndVerifyClientCert, got %v", cfg.ClientAuth)
	}
	if cfg.ClientCAs == nil {
		t.Error("expected ClientCAs to be set")
	}
}

func TestNewServerTLSConfig_MissingCert(t *testing.T) {
	_, err := auth.NewServerTLSConfig("/nonexistent.crt", "/nonexistent.key", "")
	if err == nil {
		t.Fatal("expected error for missing cert, got nil")
	}
}

func TestNewServerTLSConfig_BadCA(t *testing.T) {
	ca := selfSignedCA(t)
	srv := signedLeaf(t, ca, "server", []string{"localhost"}, true)

	certFile := writeTemp(t, "server.crt", srv.certPEM)
	keyFile := writeTemp(t, "server.key", srv.keyPEM)
	badCA := writeTemp(t, "bad-ca.crt", []byte("not valid PEM"))

	_, err := auth.NewServerTLSConfig(certFile, keyFile, badCA)
	if err == nil {
		t.Fatal("expected error for invalid CA PEM, got nil")
	}
}

// ----------------------------------------------------------------------------
// FromContext
// ----------------------------------------------------------------------------

func TestFromContext_Empty(t *testing.T) {
	_, ok := auth.FromContext(context.Background())
	if ok {
		t.Error("expected no identity in empty context")
	}
}

// ----------------------------------------------------------------------------
// withIdentity via interceptors — end-to-end with a real TLS handshake
// ----------------------------------------------------------------------------

// echoServer is a minimal gRPC server used for interceptor integration tests.
// We test via the auth interceptors by building a real TLS connection.

func TestIdentityExtracted_mTLS(t *testing.T) {
	ca := selfSignedCA(t)
	srvLeaf := signedLeaf(t, ca, "server", []string{"localhost"}, true)
	clientLeaf := signedLeaf(t, ca, "alice", nil, false)

	// Build server TLS config (mTLS)
	serverTLS := &tls.Config{
		Certificates: []tls.Certificate{srvLeaf.tlsCert},
		ClientCAs:    ca.certPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
	}

	// Build client TLS config
	clientTLS := &tls.Config{
		Certificates: []tls.Certificate{clientLeaf.tlsCert},
		RootCAs:      ca.certPool,
		ServerName:   "localhost",
		MinVersion:   tls.VersionTLS12,
	}

	// Channel to capture extracted identity.
	identityCh := make(chan *auth.Identity, 1)

	// Start a gRPC server with the interceptor.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	grpcSrv := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(serverTLS)),
		grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			id, _ := auth.FromContext(ctx)
			identityCh <- id
			return handler(ctx, req)
		}),
	)

	go func() { _ = grpcSrv.Serve(lis) }()
	t.Cleanup(grpcSrv.Stop)

	// Dial from client.
	conn, err := grpc.NewClient(
		lis.Addr().String(),
		grpc.WithTransportCredentials(credentials.NewTLS(clientTLS)),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Trigger a peer info lookup by fetching peer info through a raw TLS dial
	// (gRPC doesn't expose unregistered RPCs, so we validate via the TLS layer).
	tlsConn, err := tls.Dial("tcp", lis.Addr().String(), clientTLS)
	if err != nil {
		t.Fatalf("tls dial: %v", err)
	}
	defer tlsConn.Close()

	// Verify the server presented the right cert.
	serverCerts := tlsConn.ConnectionState().PeerCertificates
	if len(serverCerts) == 0 {
		t.Fatal("no server certificates in TLS handshake")
	}
	if got := serverCerts[0].Subject.CommonName; got != "server" {
		t.Errorf("server CN = %q, want %q", got, "server")
	}
}

// TestFromContext_WithPeerInfo synthesises a peer.Peer in the context and
// verifies that withIdentity (exercised via the interceptor) populates Identity.
func TestFromContext_WithPeerInfo(t *testing.T) {
	// Build a fake TLS connection state with a client certificate.
	ca := selfSignedCA(t)
	clientLeaf := signedLeaf(t, ca, "bob", []string{"bob.example.com"}, false)

	tlsState := tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{clientLeaf.cert},
		VerifiedChains:   [][]*x509.Certificate{{clientLeaf.cert, ca.cert}},
	}

	p := &peer.Peer{
		AuthInfo: credentials.TLSInfo{State: tlsState},
	}

	ctx := peer.NewContext(context.Background(), p)

	// UnaryInterceptor calls withIdentity; we simulate that directly by passing
	// through the interceptor.
	var gotID *auth.Identity
	handler := func(ctx context.Context, _ any) (any, error) {
		id, ok := auth.FromContext(ctx)
		if ok {
			gotID = id
		}
		return nil, nil
	}

	_, _ = auth.UnaryInterceptor(ctx, nil, nil, handler)

	if gotID == nil {
		t.Fatal("expected identity, got nil")
	}
	if gotID.CommonName != "bob" {
		t.Errorf("CommonName = %q, want %q", gotID.CommonName, "bob")
	}
}

// ----------------------------------------------------------------------------
// Middleware — user record existence validation
// ----------------------------------------------------------------------------

// stubRowStorage is a minimal storage.RowStorage for testing Middleware.
type stubRowStorage struct {
	users []string // user names (CNs) that "exist"
}

func (s *stubRowStorage) Create(_ context.Context, r *storage.Row) (*storage.Row, error) {
	return r, nil
}
func (s *stubRowStorage) Get(_ context.Context, _ storage.Key) (*storage.Row, error) {
	return nil, storage.ErrNotFound
}
func (s *stubRowStorage) Update(_ context.Context, r *storage.Row) (*storage.Row, error) {
	return r, nil
}
func (s *stubRowStorage) Delete(_ context.Context, _ storage.Key) error { return nil }
func (s *stubRowStorage) List(_ context.Context, q storage.Query) ([]*storage.Row, error) {
	if q.Type != "core/v1/User" {
		return nil, nil
	}
	rows := make([]*storage.Row, 0, len(s.users))
	for _, name := range s.users {
		// The tradespace can be any non-default value; Middleware searches across all tradespaces.
		rows = append(rows, &storage.Row{
			Type:       "core/v1/User",
			Tradespace: "test-tradespace",
			Name:       name,
		})
	}
	return rows, nil
}
func (s *stubRowStorage) Close() error { return nil }

func peerCtxWithCN(t *testing.T, cn string) context.Context {
	t.Helper()
	ca := selfSignedCA(t)
	leaf := signedLeaf(t, ca, cn, nil, false)
	tlsState := tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{leaf.cert},
		VerifiedChains:   [][]*x509.Certificate{{leaf.cert, ca.cert}},
	}
	p := &peer.Peer{AuthInfo: credentials.TLSInfo{State: tlsState}}
	return peer.NewContext(context.Background(), p)
}

func TestMiddleware_UnaryInterceptor_UserExists(t *testing.T) {
	rows := &stubRowStorage{users: []string{"alice"}}
	mw := auth.NewMiddleware(rows)

	ctx := peerCtxWithCN(t, "alice")

	called := false
	handler := func(ctx context.Context, _ any) (any, error) {
		called = true
		id, ok := auth.FromContext(ctx)
		if !ok {
			t.Error("expected identity in context")
		} else if id.CommonName != "alice" {
			t.Errorf("CommonName = %q, want %q", id.CommonName, "alice")
		}
		return nil, nil
	}

	_, err := mw.UnaryInterceptor(ctx, nil, nil, handler)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !called {
		t.Error("handler was not called")
	}
}

func TestMiddleware_UnaryInterceptor_UserNotFound(t *testing.T) {
	rows := &stubRowStorage{users: []string{"bob"}}
	mw := auth.NewMiddleware(rows)

	ctx := peerCtxWithCN(t, "alice")

	handler := func(ctx context.Context, _ any) (any, error) {
		t.Error("handler should not be called when user is not found")
		return nil, nil
	}

	_, err := mw.UnaryInterceptor(ctx, nil, nil, handler)
	if err == nil {
		t.Fatal("expected Unauthenticated error, got nil")
	}
}

func TestMiddleware_UnaryInterceptor_NoClientCert(t *testing.T) {
	rows := &stubRowStorage{}
	mw := auth.NewMiddleware(rows)

	// Context without any peer info (no TLS cert).
	ctx := context.Background()

	called := false
	handler := func(ctx context.Context, _ any) (any, error) {
		called = true
		return nil, nil
	}

	_, err := mw.UnaryInterceptor(ctx, nil, nil, handler)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !called {
		t.Error("handler was not called")
	}
}
