package spawnhelper

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// benchmarkFDs holds extraFDs open file descriptors for the duration of the benchmark.
func benchmarkFDs(b *testing.B, extraFDs int) {
	b.Helper()

	var held []*os.File
	for range extraFDs / 2 {
		r, w, err := os.Pipe()
		require.NoError(b, err)
		held = append(held, r, w)
	}

	b.Cleanup(func() {
		for _, f := range held {
			mustClose(b, f)
		}
	})
}

// spawnFuncs returns functions spawning /bin/true directly and through the helper.
func spawnFuncs(b *testing.B, client *Client) map[string]func() {
	b.Helper()

	files := devNullFiles(b)
	req, err := NewRequest("/bin/true", []string{"true"}, nil, "")
	require.NoError(b, err)

	return map[string]func(){
		"direct": func() {
			cmd := exec.CommandContext(b.Context(), "/bin/true")
			cmd.Stdin, cmd.Stdout, cmd.Stderr = files[0], files[1], files[2]
			require.NoError(b, cmd.Run())
		},
		"helper": func() {
			process, err := client.Spawn(b.Context(), req, files, -1)
			require.NoError(b, err)
			_, _, err = process.Wait()
			require.NoError(b, err)
		},
	}
}

// BenchmarkSpawn compares spawning /bin/true directly with spawning it through the helper while the
// benchmark holds a number of extra file descriptors open. It reports the CPU time per spawn as
// cpu-us/op: for direct spawns that is the time of this process and its reaped children, for spawns
// through the helper additionally the time of the helper and its reaped children. User and system
// time are summed as the kernel splits them by sampling, which is unreliable for short-lived
// children.
func BenchmarkSpawn(b *testing.B) {
	client := startClient(b)
	spawn := spawnFuncs(b, client)

	cpu := map[string]func() time.Duration{
		"direct": func() time.Duration {
			return rusageCPU(b, syscall.RUSAGE_SELF) + rusageCPU(b, syscall.RUSAGE_CHILDREN)
		},
		"helper": func() time.Duration {
			return rusageCPU(b, syscall.RUSAGE_SELF) + procCPU(b, client.Pid())
		},
	}

	for _, extraFDs := range []int{0, 10000, 30000} {
		b.Run(fmt.Sprintf("fds=%d", extraFDs), func(b *testing.B) {
			benchmarkFDs(b, extraFDs)

			for _, mode := range []string{"direct", "helper"} {
				b.Run(mode, func(b *testing.B) {
					before := cpu[mode]()
					for b.Loop() {
						spawn[mode]()
					}
					b.ReportMetric(float64((cpu[mode]()-before).Microseconds())/float64(b.N), "cpu-us/op")
				})
			}
		})
	}
}

// BenchmarkSpawnParallel spawns /bin/true from GOMAXPROCS goroutines at once, directly and through
// the helper, and reports the 50th and 99th percentile latency of a spawn including waiting for
// the process.
func BenchmarkSpawnParallel(b *testing.B) {
	client := startClient(b)
	spawn := spawnFuncs(b, client)

	for _, extraFDs := range []int{0, 10000, 30000} {
		b.Run(fmt.Sprintf("fds=%d", extraFDs), func(b *testing.B) {
			benchmarkFDs(b, extraFDs)

			for _, mode := range []string{"direct", "helper"} {
				b.Run(mode, func(b *testing.B) {
					var mu sync.Mutex
					var latencies []time.Duration

					b.RunParallel(func(pb *testing.PB) {
						var local []time.Duration
						for pb.Next() {
							start := time.Now()
							spawn[mode]()
							local = append(local, time.Since(start))
						}

						mu.Lock()
						latencies = append(latencies, local...)
						mu.Unlock()
					})

					slices.Sort(latencies)
					percentile := func(p float64) float64 {
						return float64(latencies[int(float64(len(latencies)-1)*p)].Microseconds())
					}
					b.ReportMetric(percentile(0.5), "p50-us")
					b.ReportMetric(percentile(0.99), "p99-us")
				})
			}
		})
	}
}

func rusageCPU(b *testing.B, who int) time.Duration {
	b.Helper()

	var rusage syscall.Rusage
	require.NoError(b, syscall.Getrusage(who, &rusage))
	return time.Duration(rusage.Utime.Nano() + rusage.Stime.Nano())
}

// procCPU returns the CPU time of pid and its reaped children. /proc only reports it in clock
// ticks, which are assumed to be 100 per second.
func procCPU(b *testing.B, pid int) time.Duration {
	b.Helper()

	stat, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	require.NoError(b, err)

	// Fields after the parenthesized command name start at the state, field 3. Fields 14 to 17
	// are utime, stime, cutime and cstime.
	fields := strings.Fields(string(stat[bytes.LastIndexByte(stat, ')')+1:]))
	var ticks int
	for _, field := range fields[14-3 : 17-3+1] {
		n, err := strconv.Atoi(field)
		require.NoError(b, err)
		ticks += n
	}

	return time.Duration(ticks) * 10 * time.Millisecond
}
