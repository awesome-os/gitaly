package burdenmonitor

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"gitlab.com/gitlab-org/gitaly/v19/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v19/internal/loadmonitor"
	"gitlab.com/gitlab-org/gitaly/v19/internal/log"
	"gitlab.com/gitlab-org/gitaly/v19/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v19/proto/go/gitalypb"
)

const (
	// shedTopN is the number of RPCs cancelled per shedding event.
	shedTopN = 10

	// shedInterval is the interval that must occur between each load shedding event.
	// It is possible that multiple LoadMonitor event are emitted at once, or very close to
	// each other, because some events can be co-related (memory pressure can also trigger CPU pressure).
	// We must make sure we do not shed load to aggressively during a burst of co-related event.
	// This interval is the interval that must passes after a load shedding event before another
	// one can occur, event if LoadMonitor events are received during that interval.
	shedInterval = 15 * time.Second
)

var rpcsShedTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "gitaly_loadshedder_rpcs_shed_total",
		Help: "Total number of RPCs cancelled by the load shedder",
	},
	[]string{"grpc_service", "grpc_method", "reason"},
)

// LoadShedderConfig holds the tunables for the LoadShedder. Add fields here
// when introducing new critical-threshold triggers.
type LoadShedderConfig struct {
	// PSI configures the PSI critical-pressure conditions per resource.
	PSI config.PSIPressureConfig
}

// LoadShedder cancels the highest-cost in-flight RPCs when the load monitor
// reports a critical condition. It owns its own subscription to the load
// monitor and the set of conditions that count as critical, and queries the
// burden monitor for the ranked list of in-flight RPCs to cancel.
//
// The set of RPCs eligible for shedding is whatever the BurdenMonitor's
// interceptor has registered, which is gated per-RPC by the
// featureflag.BurdenMonitorTrackCommands feature flag.
type LoadShedder struct {
	logger   log.Logger
	bm       *BurdenMonitor
	events   <-chan loadmonitor.Event
	lastShed time.Time

	// sortByCondition maps each condition name to the resource its in-flight
	// RPCs are ranked by when that condition fires.
	sortByCondition map[string]SortBy
}

// NewLoadShedder constructs a LoadShedder and registers its critical-threshold
// conditions with the load monitor. The load monitor must already be running.
// The shedder owns the set of conditions; cfg tunes their thresholds.
func NewLoadShedder(
	cfg LoadShedderConfig,
	logger log.Logger,
	lm loadmonitor.Monitor,
	bm *BurdenMonitor,
) (*LoadShedder, error) {
	shedConditions := []struct {
		condition loadmonitor.Condition
		sortBy    SortBy
	}{
		{newPSICriticalCondition(psiResourceCPU, cfg.PSI.CPU), SortByCPU},
		{newPSICriticalCondition(psiResourceMemory, cfg.PSI.Memory), SortByMemory},
		// IO isn't tracked per RPC, so IO pressure sheds by CPU.
		{newPSICriticalCondition(psiResourceIO, cfg.PSI.IO), SortByCPU},
		{newOOMKillCondition(), SortByMemory},
	}

	conditions := make([]loadmonitor.Condition, 0, len(shedConditions))
	sortByCondition := make(map[string]SortBy, len(shedConditions))
	for _, c := range shedConditions {
		conditions = append(conditions, c.condition)
		sortByCondition[c.condition.Name] = c.sortBy
	}

	events, err := lm.NotifyOn(conditions...)
	if err != nil {
		return nil, fmt.Errorf("load shedder: subscribing to load monitor: %w", err)
	}

	return &LoadShedder{
		logger:          logger,
		bm:              bm,
		events:          events,
		sortByCondition: sortByCondition,
	}, nil
}

// Start spawns a goroutine that cancels in-flight RPCs whenever a critical
// condition fires. The goroutine exits when ctx is cancelled or the events
// channel is closed.
func (ls *LoadShedder) Start(ctx context.Context) {
	go ls.run(ctx)
}

func (ls *LoadShedder) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-ls.events:
			if !ok {
				return
			}
			event = ls.preferMemory(event)
			// If lastShed is empty, it means no shed event took place yet, so we go ahead
			// Or else, make sure it is past the interval
			if ls.lastShed.IsZero() || time.Since(ls.lastShed) > shedInterval {
				ls.shed(ctx, event)
				ls.lastShed = time.Now()
			}
		}
	}
}

// preferMemory takes the events already queued behind event and returns the
// one to act on. Correlated conditions, such as an OOM kill and CPU pressure,
// often fire in the same poll and arrive in any order, and only one event is
// acted on per shedInterval. A condition that sheds by memory wins, so memory
// pressure isn't answered by shedding the top CPU users.
func (ls *LoadShedder) preferMemory(event loadmonitor.Event) loadmonitor.Event {
	for {
		select {
		case next, ok := <-ls.events:
			if !ok {
				return event
			}
			if ls.sortByCondition[next.ConditionName] == SortByMemory {
				event = next
			}
		default:
			return event
		}
	}
}

func (ls *LoadShedder) shed(ctx context.Context, event loadmonitor.Event) {
	sortBy := ls.sortByCondition[event.ConditionName]
	entries := ls.bm.GetTopNEntries(shedTopN, sortBy)
	reason := sortBy.reason()

	var cancelled int
	for _, entry := range entries {
		// Entries are ranked by usage, so once one uses none of the resource,
		// neither do the rest. They aren't adding to the pressure.
		if !usesResource(entry, sortBy) {
			break
		}
		ls.cancel(entry, reason)
		cancelled++
	}

	ls.logger.WithFields(log.Fields{
		"condition":    event.ConditionName,
		"reason":       event.Description,
		"sniped_count": cancelled,
	}).WarnContext(ctx, "load shedder cancelled in-flight RPCs")
}

// usesResource reports whether entry uses any of the resource sortBy ranks by.
// Memory only counts running commands, so an RPC whose commands haven't been
// polled yet or have already finished uses none.
func usesResource(entry *RPCEntry, sortBy SortBy) bool {
	if sortBy == SortByMemory {
		return entry.TotalMemory() > 0
	}
	return entry.TotalCPUTime() > 0
}

func (ls *LoadShedder) cancel(entry *RPCEntry, reason string) {
	err := structerr.NewResourceExhausted(
		"RPC cancelled by load shedder: %s", reason).
		WithDetail(&gitalypb.LimitError{ErrorMessage: reason})

	entry.Cancel(err)

	rpcsShedTotal.WithLabelValues(entry.ServiceName, entry.MethodName, reason).Inc()

	ls.logger.WithFields(log.Fields{
		"rpc_id":         entry.ID,
		"correlation_id": entry.CorrelationID,
		"repository":     entry.Repository,
		"reason":         reason,
		"cpu_time_ms":    entry.TotalCPUTime().Milliseconds(),
		"memory_bytes":   entry.TotalMemory(),
		"active_cmds":    entry.ActiveCommandCount(),
	}).WarnContext(entry.Context, "load shedder cancelled RPC")
}
