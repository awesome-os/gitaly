package server

import (
	"crypto/tls"
	"net"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v19/internal/bootstrap/starter"
	"gitlab.com/gitlab-org/gitaly/v19/internal/cache"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v19/internal/grpc/backchannel"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/labkit/fips"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	expcredentials "google.golang.org/grpc/experimental/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// TestFIPSTLSHandshake is a Go toolchain acceptance test for Gitaly's inbound
// TLS path under FIPS. It guards against the class of defect where a FIPS Go
// toolchain compiles cleanly but breaks TLS at runtime, as in the golang-fips
// `tls: HKDF-Expand-Label invocation failed unexpectedly` panic that shipped
// non-functional Gitaly TLS in every 19.2.x FIPS build. See
// https://gitlab.com/gitlab-org/gitaly/-/work_items/7373 and the incident
// review https://gitlab.com/gitlab-com/gl-infra/gitlab-dedicated/team/-/work_items/13590.
//
// Run it against a candidate toolchain to validate a Go bump:
//
//	FIPS_MODE=YesPlease make test TEST_PACKAGES=./internal/gitaly/server/... \
//	    TEST_OPTIONS="-run TestFIPSTLSHandshake -v"
//
// The test only runs under FIPS and, crucially, fails rather than skips when
// the binary is not actually running with FIPS crypto active. A TLS handshake
// that succeeds on a non-FIPS binary proves nothing about the FIPS toolchain,
// so a green result here is meaningful evidence that the toolchain is safe to
// ship. The TLS 1.3 case is the one the HKDF panic hit; the TLS 1.2 case
// covers the other handshake code path.
func TestFIPSTLSHandshake(t *testing.T) {
	if os.Getenv("GITALY_TESTING_ENABLE_FIPS") == "" {
		t.Skip("FIPS acceptance test; build with FIPS_MODE set (e.g. FIPS_MODE=YesPlease make test) to run it")
	}

	require.True(t, fips.Enabled(),
		"binary is not running with FIPS crypto active: a passing TLS handshake here would not exercise the FIPS backend. "+
			"Ensure the FIPS toolchain is in use and the backend is switched on (FIPS kernel or GOLANG_FIPS=1).")

	ctx := testhelper.Context(t)

	for _, tc := range []struct {
		desc    string
		version uint16
	}{
		{desc: "TLS 1.2", version: tls.VersionTLS12},
		{desc: "TLS 1.3", version: tls.VersionTLS13},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			grp, ctx := errgroup.WithContext(ctx)
			t.Cleanup(func() { assert.NoError(t, grp.Wait()) })

			certificate := testhelper.GenerateCertificate(t)

			cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{TLS: config.TLS{
				CertPath:   certificate.CertPath,
				KeyPath:    certificate.KeyPath,
				MinVersion: config.TLSVersion(tc.version),
			}}))

			// The MVCC interceptor and other RPC middleware are irrelevant
			// here: the defect is in the TLS handshake, which completes at the
			// transport layer before any interceptor runs. Pass nil middleware
			// and keep the server minimal.
			sf := NewGitalyServerFactory(
				cfg,
				testhelper.SharedLogger(t),
				backchannel.NewRegistry(),
				cache.New(cfg, config.NewLocator(cfg), testhelper.SharedLogger(t)),
				nil,
				nil,
				nil,
			)
			t.Cleanup(sf.Stop)

			srv, err := sf.CreateExternal(true)
			require.NoError(t, err)
			t.Cleanup(srv.Stop)
			healthpb.RegisterHealthServer(srv, health.NewServer())

			listener, err := (&net.ListenConfig{}).Listen(ctx, starter.TCP, "localhost:0")
			require.NoError(t, err)
			grp.Go(func() error { return srv.Serve(listener) })

			// Pin the client to exactly this TLS version so the handshake
			// exercises the intended protocol path, not a negotiated fallback.
			tlsCfg := certificate.TLSConfig(t)
			tlsCfg.MinVersion = tc.version
			tlsCfg.MaxVersion = tc.version

			cc, err := grpc.NewClient(
				listener.Addr().String(),
				grpc.WithTransportCredentials(expcredentials.NewTLSWithALPNDisabled(tlsCfg)),
			)
			require.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, cc.Close()) })

			// A broken FIPS toolchain crash-loops or panics here on the
			// inbound handshake; a working one serves the request.
			resp, err := healthpb.NewHealthClient(cc).Check(ctx, &healthpb.HealthCheckRequest{})
			require.NoError(t, err)
			require.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.GetStatus())
		})
	}
}
