package burdenmonitor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v19/internal/loadmonitor"
	"gitlab.com/gitlab-org/gitaly/v19/internal/testhelper"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type mockLoadMonitor struct {
	ch         chan loadmonitor.Event
	notifyErr  error
	conditions []loadmonitor.Condition
}

func newMockLoadMonitor() *mockLoadMonitor {
	return &mockLoadMonitor{ch: make(chan loadmonitor.Event, 4)}
}

func (m *mockLoadMonitor) Start(context.Context) error { return nil }

func (m *mockLoadMonitor) Stop() {}

func (m *mockLoadMonitor) NotifyOn(conditions ...loadmonitor.Condition) (<-chan loadmonitor.Event, error) {
	if m.notifyErr != nil {
		return nil, m.notifyErr
	}
	m.conditions = conditions
	return m.ch, nil
}

func enabledShedderConfig() LoadShedderConfig {
	resource := func(critical float64) config.PSIResourceConfig {
		return config.PSIResourceConfig{Enabled: true, CriticalThreshold: critical}
	}
	return LoadShedderConfig{
		PSI: config.PSIPressureConfig{
			CPU:    resource(50.0),
			Memory: resource(40.0),
			IO:     resource(40.0),
		},
	}
}

func TestNewLoadShedder_RegistersConditions(t *testing.T) {
	t.Parallel()

	lm := newMockLoadMonitor()
	bm := New(testhelper.SharedLogger(t))

	ls, err := NewLoadShedder(enabledShedderConfig(), testhelper.SharedLogger(t), lm, bm)
	require.NoError(t, err)
	require.NotNil(t, ls)

	// Three PSI critical conditions + one OOM kill condition.
	require.Len(t, lm.conditions, 4)

	var names []string
	for _, c := range lm.conditions {
		names = append(names, c.Name)
	}
	require.ElementsMatch(t, []string{
		"LoadShedderPSI/cpu",
		"LoadShedderPSI/memory",
		"LoadShedderPSI/io",
		"LoadShedderOOMKill",
	}, names)
}

func TestNewLoadShedder_NotifyOnError(t *testing.T) {
	t.Parallel()

	lm := &mockLoadMonitor{notifyErr: errors.New("monitor not running")}
	bm := New(testhelper.SharedLogger(t))

	_, err := NewLoadShedder(enabledShedderConfig(), testhelper.SharedLogger(t), lm, bm)
	require.ErrorContains(t, err, "subscribing to load monitor")
}

// ranking lists, by index into the entries from registerRankedEntries, which
// entries a shedding event cancels and which it keeps.
type ranking struct {
	cancelled []int
	kept      []int
}

var (
	topByCPU    = ranking{cancelled: []int{2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, kept: []int{0, 1}}
	topByMemory = ranking{cancelled: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, kept: []int{10, 11}}
)

// registerRankedEntries registers shedTopN+2 in-flight entries whose CPU and
// memory rank in opposite orders: CPU time increases with the index, memory
// decreases. The top shedTopN by either resource are deterministic and differ.
func registerRankedEntries(t *testing.T, ctx context.Context, bm *BurdenMonitor) []*RPCEntry {
	t.Helper()

	entries := make([]*RPCEntry, shedTopN+2)
	for i := range entries {
		pid := 100 + i
		_, e := bm.RegisterRPC(ctx, "/foo.Service/A")
		e.RegisterCommand(pid, "git rev-list", time.Now())
		e.Commands[pid].UserTime = time.Duration(i+1) * time.Millisecond
		e.Commands[pid].AnonRSS = int64(1000 * (len(entries) - i))
		entries[i] = e
	}
	return entries
}

func requireShed(t *testing.T, entries []*RPCEntry, expected ranking, reason string) {
	t.Helper()

	for _, i := range expected.cancelled {
		require.ErrorContains(t, context.Cause(entries[i].Context),
			"RPC cancelled by load shedder: "+reason, "expected entry %d to be cancelled", i)
	}
	for _, i := range expected.kept {
		require.NoError(t, entries[i].Context.Err(), "expected entry %d to be kept", i)
	}
}

func TestLoadShedder_EventCancelsTopEntriesByResource(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		condition string
		reason    string
		expected  ranking
	}{
		{condition: "LoadShedderPSI/cpu", reason: "high CPU usage", expected: topByCPU},
		{condition: "LoadShedderPSI/io", reason: "high CPU usage", expected: topByCPU},
		{condition: "LoadShedderPSI/memory", reason: "high memory usage", expected: topByMemory},
		{condition: "LoadShedderOOMKill", reason: "high memory usage", expected: topByMemory},
	} {
		t.Run(tc.condition, func(t *testing.T) {
			t.Parallel()

			lm := newMockLoadMonitor()
			bm := New(testhelper.SharedLogger(t))
			ctx := testhelper.Context(t)
			entries := registerRankedEntries(t, ctx, bm)

			ls, err := NewLoadShedder(enabledShedderConfig(), testhelper.SharedLogger(t), lm, bm)
			require.NoError(t, err)

			lm.ch <- loadmonitor.Event{ConditionName: tc.condition}
			close(lm.ch)

			ls.run(ctx)

			requireShed(t, entries, tc.expected, tc.reason)
		})
	}
}

func TestLoadShedder_PrefersMemoryWhenEventsArriveTogether(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc   string
		events []string
	}{
		{desc: "memory first", events: []string{"LoadShedderOOMKill", "LoadShedderPSI/cpu"}},
		{desc: "memory last", events: []string{"LoadShedderPSI/cpu", "LoadShedderPSI/io", "LoadShedderOOMKill"}},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			lm := newMockLoadMonitor()
			bm := New(testhelper.SharedLogger(t))
			ctx := testhelper.Context(t)
			entries := registerRankedEntries(t, ctx, bm)

			ls, err := NewLoadShedder(enabledShedderConfig(), testhelper.SharedLogger(t), lm, bm)
			require.NoError(t, err)

			for _, condition := range tc.events {
				lm.ch <- loadmonitor.Event{ConditionName: condition}
			}
			close(lm.ch)

			ls.run(ctx)

			requireShed(t, entries, topByMemory, "high memory usage")
		})
	}
}

func TestLoadShedder_KeepsEntriesWithoutUsage(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		condition string
		reason    string
	}{
		{condition: "LoadShedderOOMKill", reason: "high memory usage"},
		{condition: "LoadShedderPSI/cpu", reason: "high CPU usage"},
	} {
		t.Run(tc.condition, func(t *testing.T) {
			t.Parallel()

			lm := newMockLoadMonitor()
			bm := New(testhelper.SharedLogger(t))
			ctx := testhelper.Context(t)

			// Only the first three entries use any CPU or memory. The rest
			// have running commands that haven't been polled yet.
			entries := make([]*RPCEntry, shedTopN)
			for i := range entries {
				pid := 100 + i
				_, e := bm.RegisterRPC(ctx, "/foo.Service/A")
				e.RegisterCommand(pid, "git rev-list", time.Now())
				if i < 3 {
					e.Commands[pid].UserTime = time.Millisecond
					e.Commands[pid].AnonRSS = 1000
				}
				entries[i] = e
			}

			ls, err := NewLoadShedder(enabledShedderConfig(), testhelper.SharedLogger(t), lm, bm)
			require.NoError(t, err)

			lm.ch <- loadmonitor.Event{ConditionName: tc.condition}
			close(lm.ch)

			ls.run(ctx)

			requireShed(t, entries, ranking{
				cancelled: []int{0, 1, 2},
				kept:      []int{3, 4, 5, 6, 7, 8, 9},
			}, tc.reason)
		})
	}
}

func TestLoadShedder_ClosedChannelExitsGoroutine(t *testing.T) {
	t.Parallel()

	lm := newMockLoadMonitor()
	bm := New(testhelper.SharedLogger(t))

	ls, err := NewLoadShedder(enabledShedderConfig(), testhelper.SharedLogger(t), lm, bm)
	require.NoError(t, err)

	close(lm.ch)
	ls.run(testhelper.Context(t))
}

func TestLoadShedder_StartProcessesEventsAsynchronously(t *testing.T) {
	t.Parallel()

	lm := newMockLoadMonitor()
	bm := New(testhelper.SharedLogger(t))

	ctx, cancel := context.WithCancel(testhelper.Context(t))
	defer cancel()

	_, entry := bm.RegisterRPC(ctx, "/foo.Service/A")
	entry.RegisterCommand(101, "git rev-list", time.Now())
	entry.Commands[101].AnonRSS = 1000

	ls, err := NewLoadShedder(enabledShedderConfig(), testhelper.SharedLogger(t), lm, bm)
	require.NoError(t, err)

	ls.Start(ctx)

	lm.ch <- loadmonitor.Event{
		ConditionName: "LoadShedderOOMKill",
		Description:   eventLoadShedderOOMKill,
	}

	// The shedder runs in a goroutine, so wait for the cancellation to
	// propagate to the entry's context.
	select {
	case <-entry.Context.Done():
	case <-time.After(time.Second):
		t.Fatal("entry was not cancelled by the LoadShedder goroutine within 1s")
	}

	// The Done channel fires for any cancellation; verify the cause came
	// from the shedder by checking it is a RESOURCE_EXHAUSTED structerr
	// referencing the shedder, not test/parent context cleanup.
	cause := context.Cause(entry.Context)
	require.Error(t, cause)
	require.ErrorContains(t, cause, "RPC cancelled by load shedder")
	require.Equal(t, codes.ResourceExhausted, status.Code(cause))
}

func TestLoadShedder_ContextCancellationExitsGoroutine(t *testing.T) {
	t.Parallel()

	lm := newMockLoadMonitor()
	bm := New(testhelper.SharedLogger(t))

	ls, err := NewLoadShedder(enabledShedderConfig(), testhelper.SharedLogger(t), lm, bm)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		ls.run(ctx)
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("LoadShedder.run did not exit after context cancellation")
	}
}
