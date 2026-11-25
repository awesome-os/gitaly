package hook

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v19/internal/git/pktline"
	gitalyhook "gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v19/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v19/internal/log"
	"gitlab.com/gitlab-org/gitaly/v19/internal/packfileuri"
	"gitlab.com/gitlab-org/gitaly/v19/internal/stream"
	"gitlab.com/gitlab-org/gitaly/v19/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v19/proto/go/gitalypb"
	"google.golang.org/protobuf/encoding/protojson"
)

var (
	// packObjectsCommands is a counter that is incremented everytime a git-pack-objects
	// command was run or attempted to be run.
	//
	// The label `run` is a boolean value indicating if git-pack-objects was run (true) or was
	// not run because of an error prior to running the command (in that case it is false)
	//
	// The label `error` is a boolean value indicating if the command returned an error. For this value
	// to be `false`, `run` needs to be `true` and the command should have exited without error.
	//
	// The label `packfile_uri` is a boolean value indicating if packfile-uri was used or not
	// for a particular command instance.
	packObjectsCommands = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gitaly_pack_objects_command_total",
		Help: "Number of time a pack-object command was run",
	}, []string{"run", "error", "packfile_uri"})
	packObjectsServedBytes = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gitaly_pack_objects_served_bytes_total",
		Help: "Number of bytes of git-pack-objects data served to clients",
	})
	packObjectsCacheLookups = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gitaly_pack_objects_cache_lookups_total",
		Help: "Number of lookups in the PackObjectsHook cache, divided by hit/miss",
	}, []string{"result"})
	packObjectsGeneratedBytes = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gitaly_pack_objects_generated_bytes_total",
		Help: "Number of bytes generated in PackObjectsHook by running git-pack-objects",
	})
)

func (s *server) packObjectsHook(ctx context.Context, req *gitalypb.PackObjectsHookWithSidechannelRequest, args *packObjectsArgs, stdinReader io.Reader, output io.Writer) error {
	cacheKey, stdin, err := s.computeCacheKey(ctx, req, stdinReader)
	if err != nil {
		return err
	}

	// We do not know yet who has to close stdin. In case of a cache hit, it
	// is us. In case of a cache miss, a separate goroutine will run
	// git-pack-objects, and that goroutine may outlive the current request.
	// In that case, that separate goroutine will be responsible for closing
	// stdin.
	closeStdin := true
	defer func() {
		if closeStdin {
			stdin.Close()
		}
	}()

	servedBytes, created, err := s.packObjectsCache.Fetch(ctx, cacheKey, output, func(w io.Writer) error {
		ipAddr := net.ParseIP(req.GetRemoteIp())
		if ipAddr == nil {
			// Best effort, maybe the remote IP includes source port
			if ip, _, err := net.SplitHostPort(req.GetRemoteIp()); err == nil {
				ipAddr = net.ParseIP(ip)
			}
		}
		// Ignore loop-back IPs
		if ipAddr != nil && !ipAddr.IsLoopback() {
			return s.runPackObjectsLimited(
				ctx,
				w,
				ipAddr.String(),
				req,
				args,
				stdin,
				cacheKey,
			)
		}

		return s.runPackObjects(ctx, w, req, args, stdin, cacheKey)
	})
	if err != nil {
		return err
	}

	if created {
		closeStdin = false
		packObjectsCacheLookups.WithLabelValues("miss").Inc()
	} else {
		packObjectsCacheLookups.WithLabelValues("hit").Inc()
	}

	customFields := log.CustomFieldsFromContext(ctx)
	if customFields != nil {
		customFields.RecordMetadata("pack_objects_cache.key", cacheKey)
		customFields.RecordSum("pack_objects_cache.served_bytes", int(servedBytes))
		if created {
			customFields.RecordMetadata("pack_objects_cache.hit", "false")
		} else {
			customFields.RecordMetadata("pack_objects_cache.hit", "true")
		}
	}
	packObjectsServedBytes.Add(float64(servedBytes))

	return nil
}

// computeCacheKey returns the cache key used for caching pack-objects. A cache key is made up of
// both the requested objects and essential parameters that could impact the content of the
// generated packfile. Including any insignificant information could result in a lower cache hit rate.
func (s *server) computeCacheKey(ctx context.Context, req *gitalypb.PackObjectsHookWithSidechannelRequest, stdinReader io.Reader) (string, io.ReadCloser, error) {
	cacheHash := sha256.New()

	repository := req.GetRepository()
	if tx := storage.ExtractTransaction(ctx); tx != nil {
		// The cache uses the requests as the keys. As the request's repository in the RPC handler has been rewritten
		// to point to the transaction's repository, the handler sees each request as different even if they point to
		// the same repository. Restore the original request to ensure identical requests get the same key.
		repository = tx.OriginalRepository(req.GetRepository())
	}

	cacheKeyPrefix, err := protojson.Marshal(&gitalypb.PackObjectsHookWithSidechannelRequest{
		Repository:  repository,
		Args:        req.GetArgs(),
		GitProtocol: req.GetGitProtocol(),
	})
	if err != nil {
		return "", nil, err
	}
	if _, err := cacheHash.Write(cacheKeyPrefix); err != nil {
		return "", nil, err
	}
	stdin, err := bufferStdin(stdinReader, cacheHash)
	if err != nil {
		return "", nil, err
	}
	cacheKey := hex.EncodeToString(cacheHash.Sum(nil))
	return cacheKey, stdin, nil
}

func (s *server) runPackObjects(
	ctx context.Context,
	w io.Writer,
	req *gitalypb.PackObjectsHookWithSidechannelRequest,
	args *packObjectsArgs,
	stdin io.ReadCloser,
	key string,
) error {
	// We want to keep the context for logging, but we want to block all its
	// cancellation signals (deadline, cancel etc.). This is because of
	// the following scenario. Imagine client1 calls PackObjectsHook and
	// causes runPackObjects to run in a goroutine. Now suppose that client2
	// calls PackObjectsHook with the same arguments and stdin, so it joins
	// client1 in waiting for this goroutine. Now client1 hangs up before the
	// runPackObjects goroutine is done.
	//
	// If the cancellation of client1 propagated into the runPackObjects
	// goroutine this would affect client2. We don't want that. So to prevent
	// that, we suppress the cancellation of the originating context.
	ctx = context.WithoutCancel(ctx)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Cancel the command once the graceful shutdown period has elapsed. The
	// context above intentionally ignores the per-request cancellation so that
	// one client hanging up does not abort a git-pack-objects process shared
	// with other clients via the streamcache. Process shutdown is different: we
	// must be able to terminate the command, otherwise a slow, lossy, or stalled
	// transfer keeps git-pack-objects alive well past the shutdown period. The
	// returned stop function unregisters the callback once the command finishes,
	// so nothing leaks; AfterFunc on a non-cancellable shutdown context is a
	// no-op.
	defer context.AfterFunc(s.shutdownCtx, cancel)()

	defer stdin.Close()

	return s.runPackObjectsFn(ctx, s.gitCmdFactory, w, req, args, stdin, key, s.packfileURIManager, s.logger)
}

func (s *server) runPackObjectsLimited(
	ctx context.Context,
	w io.Writer,
	limitkey string,
	req *gitalypb.PackObjectsHookWithSidechannelRequest,
	args *packObjectsArgs,
	stdin io.ReadCloser,
	key string,
) error {
	ctx = context.WithoutCancel(ctx)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// See runPackObjects for why the command is cancelled on shutdown even though
	// it otherwise ignores the per-request cancellation.
	defer context.AfterFunc(s.shutdownCtx, cancel)()

	defer stdin.Close()

	if _, err := s.packObjectsLimiter.Limit(
		ctx,
		limitkey,
		func() (interface{}, error) {
			return nil,
				s.runPackObjectsFn(
					ctx,
					s.gitCmdFactory,
					w,
					req,
					args,
					stdin,
					key,
					s.packfileURIManager,
					s.logger,
				)
		},
	); err != nil {
		return err
	}

	return nil
}

func runPackObjects(
	ctx context.Context,
	gitCmdFactory gitcmd.CommandFactory,
	w io.Writer,
	req *gitalypb.PackObjectsHookWithSidechannelRequest,
	args *packObjectsArgs,
	stdin io.Reader,
	key string,
	packfileURIMgr *packfileuri.Manager,
	logger log.Logger,
) (err error) {
	repo := req.GetRepository()

	counter := &helper.CountingWriter{W: w}
	sw := pktline.NewSidebandWriter(counter)
	stdout := bufio.NewWriterSize(sw.Writer(stream.BandStdout), pktline.MaxSidebandData)
	stderrBuf := &bytes.Buffer{}
	stderr := io.MultiWriter(sw.Writer(stream.BandStderr), stderrBuf)

	packfileURIUsed := packfileURIMgr != nil && args.hasURIProtocol()
	commandRun := false

	defer func() {
		packObjectsGeneratedBytes.Add(float64(counter.N))
		customFields := log.CustomFieldsFromContext(ctx)
		if customFields != nil {
			customFields.RecordMetadata("pack_objects_cache.key", key)
			customFields.RecordSum("pack_objects_cache.generated_bytes", int(counter.N))
			if total := totalMessage(stderrBuf.Bytes()); total != "" {
				customFields.RecordMetadata("pack_objects.compression_statistics", total)
			}
		}

		packObjectsCommands.WithLabelValues(
			strconv.FormatBool(commandRun),
			strconv.FormatBool(err != nil),
			strconv.FormatBool(packfileURIUsed),
		).Inc()
	}()

	if packfileURIUsed {
		// stdin is normally the temp file bufferStdin made, so we can
		// scan the negotiation straight off it and seek back to the
		// start before pack-objects replays it on fallback. A reader
		// that can't seek gets buffered in memory instead.
		seeker, ok := stdin.(io.ReadSeeker)
		if !ok {
			stdinData, err := io.ReadAll(stdin)
			if err != nil {
				return fmt.Errorf("read pack-objects stdin: %w", err)
			}
			seeker = bytes.NewReader(stdinData)
		}

		negotiation, err := scanNegotiation(seeker)
		if err != nil {
			return fmt.Errorf("scan pack-objects stdin: %w", err)
		}
		if _, err := seeker.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf("rewind pack-objects stdin: %w", err)
		}
		stdin = seeker

		// The static path hands out every pack the manifest names, so it is
		// only correct for a plain full clone. Anything that narrows the pack —
		// haves after --not, a shallow boundary (--shallow stdin lines, the
		// shape a v2 depth-limited clone sends, or the --shallow-file
		// argument), or an object filter — needs a computed subset the
		// manifest does not store.
		staticServable := !negotiation.hasExclusions && !negotiation.hasShallow &&
			!args.shallowFile && !args.hasFilter()

		if !staticServable {
			// When the dynamic-pack opt-in is on, materialize the computed
			// subset, cache it in object storage keyed by the negotiation, and
			// serve that; the pack-objects cache key covers the request's
			// arguments (including filters) and the whole negotiation, so a
			// cached pack only ever answers an identical request.
			if packfileURIMgr.DynamicPacksEnabled() {
				tmpDir, derr := os.MkdirTemp("", "mvcc-shallow-pack-")
				if derr != nil {
					return fmt.Errorf("tempdir for shallow pack: %w", derr)
				}
				defer func() { _ = os.RemoveAll(tmpDir) }()

				produce := func() (string, string, error) {
					return assembleShallowPack(ctx, gitCmdFactory, repo, args, seeker, tmpDir)
				}
				served, format, derr := packfileURIMgr.ServeDynamicShallowPack(ctx, req.GetRepository(), key, produce,
					func(uri packfileuri.PackfileURI) error {
						if _, err := fmt.Fprintf(stdout, "%s %s\n", uri.PackHash, uri.URL); err != nil {
							return fmt.Errorf("write packfile-uri line: %w", err)
						}
						return stdout.Flush()
					})
				if derr != nil {
					return fmt.Errorf("serve dynamic shallow pack: %w", derr)
				}
				if served {
					pack, perr := emptyPack(format)
					if perr != nil {
						return perr
					}
					commandRun = true
					if _, err := stdout.Write(pack); err != nil {
						return fmt.Errorf("write empty pack: %w", err)
					}
					if err := stdout.Flush(); err != nil {
						return fmt.Errorf("flush stdout: %w", err)
					}
					logger.DebugContext(ctx, "packfile-uri: served dynamic shallow pack, skipped inline")
					return nil
				}
				// Fallback (any pre-write failure: probe, assembly, upload or
				// signing): the seeker may have been consumed by produce, so
				// rewind before the inline pack-objects.
				if _, err := seeker.Seek(0, io.SeekStart); err != nil {
					return fmt.Errorf("rewind pack-objects stdin: %w", err)
				}
				stdin = seeker
			}
			logger.DebugContext(ctx, "packfile-uri: serving inline pack")
		} else {
			served, format, err := packfileURIMgr.ServePackfileURIs(ctx, req.GetRepository(), func(uri packfileuri.PackfileURI) error {
				if _, err := fmt.Fprintf(stdout, "%s %s\n", uri.PackHash, uri.URL); err != nil {
					return fmt.Errorf("write packfile-uri line: %w", err)
				}
				// Flush per line so upload-pack sees output while the next
				// URL is still being signed; buffering the whole list stalls
				// the response for the full signing time.
				return stdout.Flush()
			})
			if err != nil {
				return fmt.Errorf("serve packfile URIs: %w", err)
			}

			commandRun = true
			if served > 0 {
				pack, perr := emptyPack(format)
				if perr != nil {
					return perr
				}
				if _, err := stdout.Write(pack); err != nil {
					return fmt.Errorf("write empty pack: %w", err)
				}
				if err := stdout.Flush(); err != nil {
					return fmt.Errorf("flush stdout: %w", err)
				}
				logger.WithField("packfile_uris", served).DebugContext(ctx, "packfile-uri: served URIs, skipped inline pack-objects")
				return nil
			}
		}
	}

	cmd, err := gitCmdFactory.New(ctx, repo, args.subcmd(),
		gitcmd.WithStdin(stdin),
		gitcmd.WithStdout(stdout),
		gitcmd.WithStderr(stderr),
		gitcmd.WithGlobalOption(args.globals()...),
	)
	if err != nil {
		return err
	}

	commandRun = true
	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("git-pack-objects: stderr: %q err: %w", stderrBuf.String(), err)
	}
	if err := stdout.Flush(); err != nil {
		return fmt.Errorf("flush stdout: %w", err)
	}
	return nil
}

// negotiationInfo describes the shape of upload-pack's stdin negotiation.
type negotiationInfo struct {
	// hasExclusions is true when objects follow the "--not" separator: haves
	// or shallow edges the client already has.
	hasExclusions bool
	// hasShallow is true when the negotiation carries "--shallow <oid>" lines:
	// the client asked for a depth-limited clone, so the full pack is wrong
	// for it even when nothing follows --not (the shape a v2 depth-limited
	// clone of a whole branch produces).
	hasShallow bool
}

// scanNegotiation reads upload-pack's stdin a line at a time and reports the
// negotiation's shape without loading it whole.
func scanNegotiation(stdin io.Reader) (negotiationInfo, error) {
	var info negotiationInfo
	scanner := bufio.NewScanner(stdin)
	sawNot := false
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case !sawNot:
			if line == "--not" {
				sawNot = true
			} else if strings.HasPrefix(line, "--shallow ") {
				info.hasShallow = true
			}
		case len(line) > 0:
			info.hasExclusions = true
			return info, scanner.Err()
		}
	}
	return info, scanner.Err()
}

// assembleShallowPack runs pack-objects for a shallow request to a file in
// tmpDir and returns the pack's path and git hash (from index-pack, which is
// the name the client will compute for the URI'd pack). stdin is rewound first
// because an earlier scan consumed it.
func assembleShallowPack(ctx context.Context, gitCmdFactory gitcmd.CommandFactory, repo *gitalypb.Repository, args *packObjectsArgs, stdin io.ReadSeeker, tmpDir string) (string, string, error) {
	if _, err := stdin.Seek(0, io.SeekStart); err != nil {
		return "", "", fmt.Errorf("rewind stdin: %w", err)
	}
	packPath := filepath.Join(tmpDir, "shallow.pack")
	pf, err := os.Create(packPath)
	if err != nil {
		return "", "", fmt.Errorf("create pack file: %w", err)
	}
	// The request's flags normally include --thin: deltas against objects the
	// requesting client already has. A thin pack is not standalone — index-pack
	// below rejects its unresolved deltas without --fix-thin, and the cached
	// pack is later downloaded by clients whose haves differ from this
	// request's. Materialize a self-contained pack instead.
	subcmd := args.subcmd()
	flags := subcmd.Flags[:0]
	for _, f := range subcmd.Flags {
		if flag, ok := f.(gitcmd.Flag); ok && flag.Name == "--thin" {
			continue
		}
		flags = append(flags, f)
	}
	subcmd.Flags = flags

	var packErr bytes.Buffer
	cmd, err := gitCmdFactory.New(ctx, repo, subcmd,
		gitcmd.WithStdin(stdin),
		gitcmd.WithStdout(pf),
		gitcmd.WithStderr(&packErr),
		gitcmd.WithGlobalOption(args.globals()...),
	)
	if err != nil {
		pf.Close()
		return "", "", err
	}
	if err := cmd.Wait(); err != nil {
		pf.Close()
		return "", "", fmt.Errorf("pack-objects: %q: %w", packErr.String(), err)
	}
	if err := pf.Close(); err != nil {
		return "", "", fmt.Errorf("close pack file: %w", err)
	}

	// index-pack writes <pack>.idx and prints the pack's SHA, which is exactly
	// the name a client names the URI'd pack by.
	var hashBuf, idxErr bytes.Buffer
	icmd, err := gitCmdFactory.New(ctx, repo, gitcmd.Command{
		Name: "index-pack",
		Args: []string{packPath},
	},
		gitcmd.WithStdout(&hashBuf),
		gitcmd.WithStderr(&idxErr),
	)
	if err != nil {
		return "", "", err
	}
	if err := icmd.Wait(); err != nil {
		return "", "", fmt.Errorf("index-pack: %q: %w", idxErr.String(), err)
	}
	packHash := strings.TrimSpace(hashBuf.String())
	if len(packHash) < 40 {
		return "", "", fmt.Errorf("index-pack returned no hash: %q", packHash)
	}
	return packPath, packHash, nil
}

// emptyPack returns a valid packfile with 0 objects (PACK v2, 0 entries)
// whose trailing checksum uses the repository's object format; a SHA-256
// repository rejects a pack with a SHA-1 trailer.
func emptyPack(format packfileuri.ObjectFormat) ([]byte, error) {
	header := []byte{
		'P', 'A', 'C', 'K',
		0, 0, 0, 2, // version 2
		0, 0, 0, 0, // 0 objects
	}

	var h hash.Hash
	switch format {
	case packfileuri.ObjectFormatSHA1:
		h = sha1.New()
	case packfileuri.ObjectFormatSHA256:
		h = sha256.New()
	default:
		return nil, fmt.Errorf("no object format for empty pack trailer: %q", format)
	}
	h.Write(header)
	return append(header, h.Sum(nil)...), nil
}

func totalMessage(stderr []byte) string {
	start := bytes.Index(stderr, []byte("Total "))
	if start < 0 {
		return ""
	}

	end := bytes.Index(stderr[start:], []byte("\n"))
	if end < 0 {
		return ""
	}

	return string(stderr[start : start+end])
}

var (
	errNoPackObjects = errors.New("missing pack-objects")
	errNonFlagArg    = errors.New("non-flag argument")
	errNoStdout      = errors.New("missing --stdout")
)

func parsePackObjectsArgs(args []string) (*packObjectsArgs, error) {
	result := &packObjectsArgs{}

	// Check for special argument used with shallow clone:
	// https://gitlab.com/gitlab-org/git/-/blob/v2.30.0/upload-pack.c#L287-290
	if len(args) >= 2 && args[0] == "--shallow-file" && args[1] == "" {
		result.shallowFile = true
		args = args[2:]
	}

	if len(args) < 1 || args[0] != "pack-objects" {
		return nil, errNoPackObjects
	}
	args = args[1:]

	// There should always be "--stdout" somewhere. Git-pack-objects can
	// write to a file too but we don't want that in this RPC.
	// https://gitlab.com/gitlab-org/git/-/blob/v2.30.0/upload-pack.c#L296
	seenStdout := false
	for _, a := range args {
		if !strings.HasPrefix(a, "-") {
			return nil, errNonFlagArg
		}
		if a == "--stdout" {
			seenStdout = true
		} else {
			result.flags = append(result.flags, a)
		}
	}

	if !seenStdout {
		return nil, errNoStdout
	}

	return result, nil
}

type packObjectsArgs struct {
	shallowFile bool
	flags       []string
}

// hasURIProtocol reports whether upload-pack asked pack-objects to exclude
// objects servable by URI: it passes --uri-protocol when the client advertised
// support for packfile-uris.
func (p *packObjectsArgs) hasURIProtocol() bool {
	for _, f := range p.flags {
		if strings.HasPrefix(f, "--uri-protocol=") {
			return true
		}
	}
	return false
}

// hasFilter reports whether upload-pack asked for an object filter (a partial
// clone). A filtered pack is a computed subset, so the manifest's full packs
// must not be served for it.
func (p *packObjectsArgs) hasFilter() bool {
	for _, f := range p.flags {
		if f == "--filter" || strings.HasPrefix(f, "--filter=") {
			return true
		}
	}
	return false
}

func (p *packObjectsArgs) globals() []gitcmd.GlobalOption {
	var globals []gitcmd.GlobalOption
	if p.shallowFile {
		globals = append(globals, gitcmd.ValueFlag{Name: "--shallow-file", Value: ""})
	}
	return globals
}

func (p *packObjectsArgs) subcmd() gitcmd.Command {
	sc := gitcmd.Command{
		Name:  "pack-objects",
		Flags: []gitcmd.Option{gitcmd.Flag{Name: "--stdout"}},
	}
	for _, f := range p.flags {
		sc.Flags = append(sc.Flags, gitcmd.Flag{Name: f})
	}
	return sc
}

func bufferStdin(r io.Reader, h hash.Hash) (_ io.ReadCloser, err error) {
	f, err := os.CreateTemp("", "PackObjectsHook-stdin")
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			f.Close()
		}
	}()

	if err := os.Remove(f.Name()); err != nil {
		return nil, err
	}

	_, err = io.Copy(f, io.TeeReader(r, h))
	if err != nil {
		return nil, err
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	return f, nil
}

func (s *server) PackObjectsHookWithSidechannel(ctx context.Context, req *gitalypb.PackObjectsHookWithSidechannelRequest) (*gitalypb.PackObjectsHookWithSidechannelResponse, error) {
	if err := s.locator.ValidateRepository(ctx, req.GetRepository()); err != nil {
		return nil, structerr.NewInvalidArgument("%w", err)
	}

	args, err := parsePackObjectsArgs(req.GetArgs())
	if err != nil {
		return nil, structerr.NewInvalidArgument("invalid pack-objects command: %v: %w", req.GetArgs(), err)
	}

	c, err := gitalyhook.GetSidechannel(ctx)
	if err != nil {
		if errors.As(err, &gitalyhook.InvalidSidechannelAddressError{}) {
			return nil, structerr.NewInvalidArgument("%w", err)
		}
		return nil, structerr.NewInternal("get side-channel: %w", err)
	}
	defer c.Close()

	if err := s.packObjectsHook(ctx, req, args, c, c); err != nil {
		if errors.Is(err, syscall.EPIPE) {
			// EPIPE is the error we get if we try to write to c after the client has
			// closed its side of the connection. By convention, we label server side
			// errors caused by the client disconnecting with the Canceled gRPC code.
			err = structerr.NewCanceled("%w", err)
		}
		return nil, structerr.NewInternal("pack objects hook: %w", err)
	}

	if err := c.Close(); err != nil {
		return nil, structerr.NewInternal("close side-channel: %w", err)
	}

	return &gitalypb.PackObjectsHookWithSidechannelResponse{}, nil
}
