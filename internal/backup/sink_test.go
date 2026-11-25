package backup

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/storage/mode"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper"

	_ "gocloud.dev/blob/memblob"
)

func TestResolveSink(t *testing.T) {
	ctx := testhelper.Context(t)

	isStorageServiceSink := func(bucketTy interface{}) func(t *testing.T, sink *Sink) {
		return func(t *testing.T, sink *Sink) {
			t.Helper()

			require.True(t, sink.bucket.As(bucketTy))
		}
	}

	tmpDir := testhelper.TempDir(t)
	gsCreds := filepath.Join(tmpDir, "gs.creds")

	var (
		azureBucket    *container.Client
		gcsBucket      *storage.Client
		s3Bucket       *s3.Client
		fileblobBucket os.FileInfo
	)

	require.NoError(t, os.WriteFile(gsCreds, []byte(`
{
  "type": "service_account",
  "project_id": "example-test-project",
  "private_key_id": "0000000000000000000000000000000000000000",
  "private_key": "test-omitted-key",
  "client_email": "test@example.invalid",
  "client_id": "000000000000000000000",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://accounts.google.com/o/oauth2/token"
}`), mode.File))

	for _, tc := range []struct {
		desc   string
		envs   map[string]string
		path   string
		verify func(t *testing.T, sink *Sink)
		errMsg string
	}{
		{
			desc: "AWS S3",
			envs: map[string]string{
				"AWS_ACCESS_KEY_ID":     "test",
				"AWS_SECRET_ACCESS_KEY": "test",
				"AWS_REGION":            "us-east-1",
			},
			path:   "s3://bucket",
			verify: isStorageServiceSink(&s3Bucket),
		},
		{
			desc: "Google Cloud Storage",
			envs: map[string]string{
				"GOOGLE_APPLICATION_CREDENTIALS": gsCreds,
			},
			path:   "blob+gs://bucket",
			verify: isStorageServiceSink(&gcsBucket),
		},
		{
			desc: "Azure Cloud File Storage",
			envs: map[string]string{
				"AZURE_STORAGE_ACCOUNT":   "test",
				"AZURE_STORAGE_KEY":       "test",
				"AZURE_STORAGE_SAS_TOKEN": "test",
			},
			path:   "blob+bucket+azblob://bucket",
			verify: isStorageServiceSink(&azureBucket),
		},
		{
			desc:   "Fileblob",
			path:   "file://" + tmpDir,
			verify: isStorageServiceSink(&fileblobBucket),
		},
		{
			desc:   "Non-existent Fileblob",
			path:   "file://" + filepath.Join(tmpDir, "some-new-dir"),
			verify: isStorageServiceSink(&fileblobBucket),
		},
		{
			desc:   "Unspecified scheme uses fileblob",
			path:   tmpDir,
			verify: isStorageServiceSink(&fileblobBucket),
		},
		{
			desc:   "undefined",
			path:   "some:invalid:path\x00",
			errMsg: `parse "some:invalid:path\x00": net/url: invalid control character in URL`,
		},
		{
			desc:   "unrecognized scheme",
			path:   "minio://bucket",
			errMsg: `unsupported sink URI scheme: "minio"`,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			for k, v := range tc.envs {
				t.Setenv(k, v)
			}

			sink, err := ResolveSink(ctx, tc.path)
			if tc.errMsg != "" {
				require.EqualError(t, err, tc.errMsg)
				return
			}

			require.NoError(t, err)
			defer testhelper.MustClose(t, sink)

			tc.verify(t, sink)
		})
	}
}

func TestFileBlobSink(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	tmpDir := testhelper.TempDir(t)
	tmpPath := filepath.Join(tmpDir, "another-dir")

	sink, err := ResolveSink(ctx, fmt.Sprintf("file://%s", tmpPath))
	defer testhelper.MustClose(t, sink)
	require.NoError(t, err)

	info, err := os.Stat(tmpPath)
	require.NoError(t, err)
	require.Equal(t, mode.Directory, info.Mode())
}

func TestStorageServiceSink(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	sss, err := ResolveSink(ctx, "mem://test_bucket")
	require.NoError(t, err)
	defer func() { require.NoError(t, sss.Close()) }()

	t.Run("write and retrieve", func(t *testing.T) {
		const relativePath = "path/to/data"

		data := []byte("test")

		w, err := sss.GetWriter(ctx, relativePath)
		require.NoError(t, err)

		_, err = io.Copy(w, bytes.NewReader(data))
		require.NoError(t, err)

		require.NoError(t, w.Close())

		reader, err := sss.GetReader(ctx, relativePath)
		require.NoError(t, err)
		defer func() { require.NoError(t, reader.Close()) }()

		retrieved, err := io.ReadAll(reader)
		require.NoError(t, err)
		require.Equal(t, data, retrieved)
	})

	t.Run("not existing path", func(t *testing.T) {
		reader, err := sss.GetReader(ctx, "not-existing")
		require.Equal(t, fmt.Errorf(`sink: new reader for "not-existing": %w`, ErrDoesntExist), err)
		require.Nil(t, reader)
	})
}

func TestStorageServiceSink_SignedURL_notImplemented(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	tmpDir := testhelper.TempDir(t)

	for _, tc := range []struct {
		desc      string
		bucketURL string
	}{
		{
			desc:      "memory bucket",
			bucketURL: "mem://test_bucket",
		},
		{
			desc:      "fs bucket",
			bucketURL: tmpDir,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			sss, err := ResolveSink(ctx, tc.bucketURL)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, sss.Close()) })

			const relativePath = "path/to/data"

			data := []byte("test")

			w, err := sss.GetWriter(ctx, relativePath)
			require.NoError(t, err)

			_, err = io.Copy(w, bytes.NewReader(data))
			require.NoError(t, err)

			require.NoError(t, w.Close())

			_, err = sss.SignedURL(ctx, relativePath, 10*time.Minute)
			require.Error(t, err)
			require.Contains(t, err.Error(), "Unimplemented")
		})
	}
}

func TestSink_List(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	rootPath := filepath.Join(t.TempDir(), "sink")

	expectedObjects := []string{
		"a/b/apple",
		"a/b/banana",
		"a/b/c/carrot",
		"a/b/pineapple",
	}

	fs := fstest.MapFS{
		".":     {Mode: mode.Directory},
		"a":     {Mode: mode.Directory},
		"a/b":   {Mode: mode.Directory},
		"a/b/c": {Mode: mode.Directory},
	}

	for _, path := range expectedObjects {
		fs[path] = &fstest.MapFile{Mode: mode.File, Data: []byte(path)}
	}

	testhelper.CreateFS(t, rootPath, fs)

	sink, err := ResolveSink(ctx, rootPath)
	require.NoError(t, err)
	defer testhelper.MustClose(t, sink)

	var objects []string
	it := sink.List("a/b/")
	for it.Next(ctx) {
		objects = append(objects, it.Path())
	}
	require.NoError(t, it.Err())
	require.Equal(t, expectedObjects, objects)
}

func TestWithS3DefaultChecksumCalculation(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc     string
		inputURL string
		expected string
	}{
		{
			desc:     "no change for AWS S3 without endpoint",
			inputURL: "s3://bucket",
			expected: "s3://bucket",
		},
		{
			desc:     "no change for AWS S3 with region only",
			inputURL: "s3://bucket?region=us-east-1",
			expected: "s3://bucket?region=us-east-1",
		},
		{
			desc:     "adds default when custom endpoint is configured",
			inputURL: "s3://bucket?endpoint=minio.example.com",
			expected: "s3://bucket?endpoint=minio.example.com&request_checksum_calculation=when_required",
		},
		{
			desc:     "adds default when custom endpoint with other params",
			inputURL: "s3://bucket?region=us-east-1&endpoint=minio.example.com",
			expected: "s3://bucket?endpoint=minio.example.com&region=us-east-1&request_checksum_calculation=when_required",
		},
		{
			desc:     "preserves user-specified when_supported with endpoint",
			inputURL: "s3://bucket?endpoint=minio.example.com&request_checksum_calculation=when_supported",
			expected: "s3://bucket?endpoint=minio.example.com&request_checksum_calculation=when_supported",
		},
		{
			desc:     "preserves user-specified when_required with endpoint",
			inputURL: "s3://bucket?endpoint=minio.example.com&request_checksum_calculation=when_required",
			expected: "s3://bucket?endpoint=minio.example.com&request_checksum_calculation=when_required",
		},
		{
			desc:     "preserves user-specified checksum for AWS S3 without endpoint",
			inputURL: "s3://bucket?request_checksum_calculation=when_supported",
			expected: "s3://bucket?request_checksum_calculation=when_supported",
		},
		{
			desc:     "handles bucket with path and custom endpoint",
			inputURL: "s3://bucket/prefix/path?endpoint=minio.example.com",
			expected: "s3://bucket/prefix/path?endpoint=minio.example.com&request_checksum_calculation=when_required",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			parsed, err := url.Parse(tc.inputURL)
			require.NoError(t, err)

			result := withS3DefaultChecksumCalculation(parsed)

			// Parse both URLs to compare query params regardless of order
			resultParsed, err := url.Parse(result)
			require.NoError(t, err)
			expectedParsed, err := url.Parse(tc.expected)
			require.NoError(t, err)

			require.Equal(t, expectedParsed.Scheme, resultParsed.Scheme)
			require.Equal(t, expectedParsed.Host, resultParsed.Host)
			require.Equal(t, expectedParsed.Path, resultParsed.Path)
			require.Equal(t, expectedParsed.Query(), resultParsed.Query())
		})
	}
}

// TestS3SinkUploadSendsUnsignedPayload is a regression test for a gocloud.dev
// v0.46.0 defect (fixed in v0.47.0) where the s3blob writer did not thread
// request_checksum_calculation=when_required into the transfer manager, so
// uploads went out with aws-chunked streaming framing and
// x-amz-content-sha256: STREAMING-UNSIGNED-PAYLOAD-TRAILER. Stricter
// S3-compatible stores (e.g. Dell ECS) reject that with HTTP 400
// XAmzContentSHA256Mismatch. With when_required honored, the request must
// carry a plain x-amz-content-sha256: UNSIGNED-PAYLOAD and no aws-chunked
// framing. This asserts on the signing headers directly so a future
// dependency bump cannot silently reintroduce the regression. Note that
// MinIO/SeaweedFS accept the streaming format, so an upload against them
// would not catch this; only header assertion does.
func TestS3SinkUploadSendsUnsignedPayload(t *testing.T) {
	ctx := testhelper.Context(t)

	var (
		mu         sync.Mutex
		putHeaders http.Header
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			mu.Lock()
			if putHeaders == nil {
				putHeaders = r.Header.Clone()
			}
			mu.Unlock()
			// Drain the body so the SDK sees a complete upload.
			_, _ = io.Copy(io.Discard, r.Body)
			w.Header().Set("ETag", `"d41d8cd98f00b204e9800998ecf8427e"`)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	// Static credentials so the SDK signs the request via SigV4.
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	uri := fmt.Sprintf(
		"s3://test-bucket?region=us-east-1&endpoint=%s&use_path_style=true&disable_https=true",
		url.QueryEscape(srv.URL),
	)

	sink, err := ResolveSink(ctx, uri)
	require.NoError(t, err)
	defer testhelper.MustClose(t, sink)

	w, err := sink.GetWriter(ctx, "backup/001.refs")
	require.NoError(t, err)
	_, err = w.Write([]byte("some-ref-data"))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	mu.Lock()
	defer mu.Unlock()
	require.NotNil(t, putHeaders, "expected a PutObject request to reach the fake S3 server")

	// With when_required honored, no flexible checksum is added: the request
	// carries no x-amz-sdk-checksum-algorithm and no x-amz-checksum-* header.
	// The v0.46.0 regression left the transfer manager on its when_supported
	// default, which added an x-amz-sdk-checksum-algorithm: CRC32 header (and
	// an x-amz-checksum-crc32 value) even for tiny objects. On larger or
	// streamed uploads that same default forces aws-chunked framing with
	// x-amz-content-sha256: STREAMING-UNSIGNED-PAYLOAD-TRAILER, which stricter
	// S3-compatible stores (e.g. Dell ECS) reject with XAmzContentSHA256Mismatch.
	require.Empty(t, putHeaders.Get("X-Amz-Sdk-Checksum-Algorithm"),
		"when_required must not add a flexible checksum algorithm")
	for header := range putHeaders {
		require.False(t, strings.HasPrefix(strings.ToLower(header), "x-amz-checksum-"),
			"when_required must not add a checksum header, found %q", header)
	}

	// Belt and suspenders for the streamed-upload path.
	require.False(t, strings.HasPrefix(putHeaders.Get("X-Amz-Content-Sha256"), "STREAMING-"),
		"upload must not use streaming payload signing")
	require.NotContains(t, strings.ToLower(putHeaders.Get("Content-Encoding")), "aws-chunked",
		"upload must not use aws-chunked streaming framing")
	require.Empty(t, putHeaders.Get("X-Amz-Trailer"),
		"upload must not request a trailing checksum")
}
