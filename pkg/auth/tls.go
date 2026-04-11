// Package auth provides TLS-based authentication for the gRPC server.
//
// Authentication model:
//   - The server is started with a TLS certificate/key pair.
//   - When a CA certificate is also provided, the server requires clients to
//     present a certificate signed by that CA (mutual TLS / mTLS).
//   - The Common Name (CN) of the verified client certificate becomes the
//     caller's identity and is injected into the request context.
//   - A core/v1/User record with a matching name (CN) must exist in storage;
//     users are namespaced within a tradespace.
package auth

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/roysav/marketplane/pkg/storage"
)

// contextKey is an unexported type for context keys in this package.
type contextKey string

const identityKey contextKey = "tls-identity"

// Identity holds information about the authenticated caller extracted from
// their TLS client certificate.
type Identity struct {
	// CommonName is the Subject.CN of the verified client certificate.
	// It is used to look up a core/v1/User record.
	CommonName string

	// DNSNames are the SANs from the client certificate.
	DNSNames []string
}

// FromContext returns the TLS Identity stored in ctx, if any.
func FromContext(ctx context.Context) (*Identity, bool) {
	id, ok := ctx.Value(identityKey).(*Identity)
	return id, ok && id != nil
}

// NewServerTLSConfig builds a *tls.Config for the gRPC server.
//
//   - certFile / keyFile — PEM-encoded server certificate and private key (required).
//   - caFile             — PEM-encoded CA certificate. When non-empty the config
//     switches to mTLS: clients must present a certificate signed by this CA.
func NewServerTLSConfig(certFile, keyFile, caFile string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("auth: load server cert/key: %w", err)
	}

	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	if caFile != "" {
		caPEM, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("auth: read CA cert %q: %w", caFile, err)
		}

		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("auth: failed to parse CA cert %q", caFile)
		}

		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return cfg, nil
}

// ServerCredentials returns gRPC TransportCredentials built from certFile/keyFile
// and, when caFile is non-empty, configured for mutual TLS.
func ServerCredentials(certFile, keyFile, caFile string) (credentials.TransportCredentials, error) {
	tlsCfg, err := NewServerTLSConfig(certFile, keyFile, caFile)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(tlsCfg), nil
}

// UnaryInterceptor is a gRPC unary server interceptor that extracts the caller's
// TLS identity and injects it into the request context.
// It does not validate user record existence; use Middleware.UnaryInterceptor for that.
func UnaryInterceptor(
	ctx context.Context,
	req any,
	_ *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	return handler(withIdentity(ctx), req)
}

// StreamInterceptor is a gRPC streaming server interceptor that extracts the
// caller's TLS identity and injects it into the stream context.
// It does not validate user record existence; use Middleware.StreamInterceptor for that.
func StreamInterceptor(
	srv any,
	ss grpc.ServerStream,
	_ *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	return handler(srv, &wrappedStream{ServerStream: ss, ctx: withIdentity(ss.Context())})
}

// Middleware validates the caller's TLS identity against stored core/v1/User records.
// When a client certificate is presented, the CN must match the Name of an existing
// core/v1/User record in any tradespace.
type Middleware struct {
	rows storage.RowStorage
}

// NewMiddleware returns a Middleware backed by the given RowStorage.
func NewMiddleware(rows storage.RowStorage) *Middleware {
	return &Middleware{rows: rows}
}

// UnaryInterceptor extracts the TLS identity, validates that a core/v1/User record
// with Name equal to the certificate CN exists, then calls the handler.
func (m *Middleware) UnaryInterceptor(
	ctx context.Context,
	req any,
	_ *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	ctx, err := m.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

// StreamInterceptor extracts the TLS identity, validates that a core/v1/User record
// with Name equal to the certificate CN exists, then calls the handler.
func (m *Middleware) StreamInterceptor(
	srv any,
	ss grpc.ServerStream,
	_ *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	ctx, err := m.authenticate(ss.Context())
	if err != nil {
		return err
	}
	return handler(srv, &wrappedStream{ServerStream: ss, ctx: ctx})
}

// authenticate injects the TLS identity into ctx and, when a CN is present,
// verifies that a matching core/v1/User record exists in storage.
func (m *Middleware) authenticate(ctx context.Context) (context.Context, error) {
	ctx = withIdentity(ctx)
	id, ok := FromContext(ctx)
	if !ok || id.CommonName == "" {
		// No client certificate presented (server-only TLS or no TLS); pass through.
		return ctx, nil
	}

	rows, err := m.rows.List(ctx, storage.Query{
		Type: "core/v1/User",
		// Empty Tradespace searches across all tradespaces.
	})
	if err != nil {
		return ctx, status.Errorf(codes.Internal, "auth: failed to look up user %q: %v", id.CommonName, err)
	}

	for _, row := range rows {
		if row.Name == id.CommonName {
			return ctx, nil
		}
	}

	return ctx, status.Errorf(codes.Unauthenticated, "auth: user %q not found", id.CommonName)
}

// withIdentity extracts TLS peer information from ctx and returns a new context
// carrying the Identity, if a valid client certificate was presented.
func withIdentity(ctx context.Context) context.Context {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return ctx
	}

	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return ctx
	}

	// Prefer the first certificate from the verified chain (mTLS path).
	if len(tlsInfo.State.VerifiedChains) > 0 && len(tlsInfo.State.VerifiedChains[0]) > 0 {
		cert := tlsInfo.State.VerifiedChains[0][0]
		return context.WithValue(ctx, identityKey, &Identity{
			CommonName: cert.Subject.CommonName,
			DNSNames:   cert.DNSNames,
		})
	}

	// Fall back to unverified peer certificates (server-only TLS, no CA configured).
	if len(tlsInfo.State.PeerCertificates) > 0 {
		cert := tlsInfo.State.PeerCertificates[0]
		return context.WithValue(ctx, identityKey, &Identity{
			CommonName: cert.Subject.CommonName,
			DNSNames:   cert.DNSNames,
		})
	}

	return ctx
}

// wrappedStream overrides Context() to return a context with the injected Identity.
type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context { return w.ctx }
