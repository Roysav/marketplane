// Package auth provides TLS-based authentication for the gRPC server.
//
// Authentication model:
//   - The server is started with a TLS certificate/key pair.
//   - When a CA certificate is also provided, the server requires clients to
//     present a certificate signed by that CA (mutual TLS / mTLS).
//   - The Common Name (CN) of the verified client certificate becomes the
//     caller's identity and is injected into the request context.
//   - A core/v1/User record with a matching commonName can carry additional
//     metadata (description) for the authenticated user.
package auth

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
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

// ClientCredentials returns gRPC TransportCredentials for a TLS client using mTLS.
func ClientCredentials(certFile, keyFile, caFile, serverName string) (credentials.TransportCredentials, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("auth: load client cert/key: %w", err)
	}

	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("auth: read CA cert %q: %w", caFile, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("auth: failed to parse CA cert %q", caFile)
	}

	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		MinVersion:   tls.VersionTLS12,
		ServerName:   serverName,
	}
	return credentials.NewTLS(cfg), nil
}

// UnaryInterceptor is a gRPC unary server interceptor that extracts the caller's
// TLS identity and injects it into the request context.
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
func StreamInterceptor(
	srv any,
	ss grpc.ServerStream,
	_ *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	return handler(srv, &wrappedStream{ServerStream: ss, ctx: withIdentity(ss.Context())})
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
