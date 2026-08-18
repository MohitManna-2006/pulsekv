package main

// Phase 9.1: LLM-serving-shaped traffic, sustained runs, and the time series a
// soak needs to prove nothing drifted.
//
// WHY THIS EXTENDS pulsekv-cluster-bench RATHER THAN FORKING IT.
//
// The load loop and the correctness proof are the same code. valueFor derives
// every value from its key index, the mixed loop verifies every read against
// it byte-for-byte, and correctnessError refuses to call a run successful if
// even one read went unverified. That machinery IS the evidence standard this
// project has used since v1's benchmark.c, and a second binary carrying its own
// copy is a second place for it to quietly weaken. The new traffic shape is
// additive and off by default: --key-distribution defaults to uniform, so every
// existing invocation -- including the node-vs-cluster overhead comparison the
// Phase 1 summary records -- runs byte-identically to before.
//
// What is genuinely new here, and could not be expressed by tuning the existing
// flags:
//
//   - A skewed key distribution. LLM serving is not uniform random: a shared
//     system prompt or a popular document means a small set of prefix blocks
//     takes most of the traffic. Uniform random over the working set measures a
//     cache that has no hot set, which is the one shape real serving never has.
//   - Several independent SDK clients. One client is one inference replica's
//     view of the cluster: its own topology refresh loop, its own connection
//     pool, its own preferred metadata replica. Sixteen goroutines sharing one
//     client measure concurrency, not multi-replica behaviour.
//   - Duration rather than an operation count, with per-interval statistics.
//     A soak asks "did this get worse over four hours", which a single
//     aggregate at the end cannot answer.

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------------
// Key selection
// ---------------------------------------------------------------------------

const (
	distributionUniform = "uniform"
	distributionZipf    = "zipf"
)

// keyChooser picks the next key index for one worker.
//
// Per-worker rather than shared: a shared chooser would need a mutex on the
// hottest line in the loop, and the point of the skew is that many workers ask
// for the same few keys at once, which is exactly when that mutex would be
// contended. Each worker seeds its own, deterministically, so a run remains
// reproducible from --seed.
type keyChooser interface {
	next() int
}

type uniformChooser struct {
	rng  *rand.Rand
	keys int
}

func (c *uniformChooser) next() int { return c.rng.Intn(c.keys) }

// zipfChooser draws from a Zipf distribution over the working set.
//
// Zipf is the standard model for popularity-ranked access and is what makes a
// small set of keys take most of the traffic. rand.Zipf generates values in
// [0, imax] with P(k) proportional to (v+k)^-s, so a larger s concentrates
// harder. The realized concentration is measured and reported rather than
// assumed from the parameter -- see concentration.
type zipfChooser struct {
	zipf *rand.Zipf
}

func (c *zipfChooser) next() int { return int(c.zipf.Uint64()) }

func newKeyChooser(distribution string, zipfS float64, keys int, seed int64) (keyChooser, error) {
	rng := rand.New(rand.NewSource(seed))
	switch distribution {
	case distributionUniform:
		return &uniformChooser{rng: rng, keys: keys}, nil
	case distributionZipf:
		// v = 1 puts the mode at rank 0. imax is the highest index, so the
		// full working set stays reachable however hard the skew.
		zipf := rand.NewZipf(rng, zipfS, 1, uint64(keys-1))
		if zipf == nil {
			return nil, fmt.Errorf("--zipf-s %g is not usable for a %d-key working set "+
				"(it must be greater than 1)", zipfS, keys)
		}
		return &zipfChooser{zipf: zipf}, nil
	default:
		return nil, fmt.Errorf("--key-distribution must be %q or %q, got %q",
			distributionUniform, distributionZipf, distribution)
	}
}

// concentration reports what the traffic shape actually was, measured from the
// keys the run really touched.
//
// Stated as evidence rather than as a parameter: "--zipf-s 1.1" tells a reader
// nothing they can check, while "the hottest 1% of keys took 43% of operations"
// is the claim that the workload was skewed, in the same terms a serving
// system's own cache statistics would use.
type concentration struct {
	TopPercent   float64 `json:"top_percent"`
	ShareOfOps   float64 `json:"share_of_ops"`
	DistinctKeys int     `json:"distinct_keys"`
	TotalOps     int64   `json:"total_ops"`
}

func measureConcentration(counts []int64, topPercent float64) concentration {
	sorted := make([]int64, 0, len(counts))
	var total int64
	distinct := 0
	for _, count := range counts {
		if count > 0 {
			distinct++
		}
		total += count
		sorted = append(sorted, count)
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] > sorted[j] })

	top := int(math.Ceil(float64(len(sorted)) * topPercent / 100))
	if top < 1 {
		top = 1
	}
	if top > len(sorted) {
		top = len(sorted)
	}
	var head int64
	for i := 0; i < top; i++ {
		head += sorted[i]
	}
	share := 0.0
	if total > 0 {
		share = 100 * float64(head) / float64(total)
	}
	return concentration{
		TopPercent:   topPercent,
		ShareOfOps:   share,
		DistinctKeys: distinct,
		TotalOps:     total,
	}
}

// ---------------------------------------------------------------------------
// Live counters and the interval time series
// ---------------------------------------------------------------------------

// liveCounters is the running view an interval reporter can read while workers
// are still running. Workers keep updating their own unsynchronised
// workerResult exactly as before; these atomics are additional, and only exist
// because a sustained run has to be observable before it ends.
type liveCounters struct {
	reads      atomic.Int64
	writes     atomic.Int64
	hits       atomic.Int64
	verified   atomic.Int64
	misses     atomic.Int64
	mismatches atomic.Int64
	rpcErrors  atomic.Int64
}

// intervalBucket collects one worker's latencies within the current reporting
// interval. A mutex per operation is irrelevant next to a network round trip,
// and it buys exact per-interval percentiles instead of an estimate.
type intervalBucket struct {
	mu     sync.Mutex
	reads  []time.Duration
	writes []time.Duration
}

func (b *intervalBucket) add(duration time.Duration, isRead bool) {
	b.mu.Lock()
	if isRead {
		b.reads = append(b.reads, duration)
	} else {
		b.writes = append(b.writes, duration)
	}
	b.mu.Unlock()
}

func (b *intervalBucket) drain() (reads, writes []time.Duration) {
	b.mu.Lock()
	reads, writes = b.reads, b.writes
	b.reads, b.writes = nil, nil
	b.mu.Unlock()
	return reads, writes
}

// intervalReport is one row of the time series.
type intervalReport struct {
	Index        int     `json:"index"`
	ElapsedSecs  float64 `json:"elapsed_seconds"`
	Operations   int64   `json:"operations"`
	Reads        int64   `json:"reads"`
	Writes       int64   `json:"writes"`
	OpsPerSecond float64 `json:"ops_per_second"`
	ReadP50Ms    float64 `json:"read_p50_ms"`
	ReadP99Ms    float64 `json:"read_p99_ms"`
	WriteP50Ms   float64 `json:"write_p50_ms"`
	WriteP99Ms   float64 `json:"write_p99_ms"`
	Verified     int64   `json:"verified"`
	Misses       int64   `json:"misses"`
	Mismatches   int64   `json:"mismatches"`
	RPCErrors    int64   `json:"rpc_errors"`
}

// ---------------------------------------------------------------------------
// Bounded sampling for long runs
// ---------------------------------------------------------------------------

// reservoir holds a uniform random sample of a stream of latencies.
//
// A fixed operation count can keep every sample, and the existing --ops path
// still does. A four-hour soak cannot: at a thousand operations a second that
// is fourteen million samples, and holding them to compute one percentile at
// the end trades a real memory risk for precision nobody needs. Algorithm R
// gives a uniform sample of known size, so the resulting percentiles are an
// honest estimate rather than "the first N operations", which would silently
// describe only the start of the run.
type reservoir struct {
	mu      sync.Mutex
	samples []latencySample
	limit   int
	seen    int64
	rng     *rand.Rand
}

func newReservoir(limit int, seed int64) *reservoir {
	return &reservoir{
		samples: make([]latencySample, 0, limit),
		limit:   limit,
		rng:     rand.New(rand.NewSource(seed)),
	}
}

func (r *reservoir) add(sample latencySample) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seen++
	if len(r.samples) < r.limit {
		r.samples = append(r.samples, sample)
		return
	}
	if index := r.rng.Int63n(r.seen); index < int64(r.limit) {
		r.samples[index] = sample
	}
}

func (r *reservoir) snapshot() ([]latencySample, int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]latencySample, len(r.samples))
	copy(out, r.samples)
	return out, r.seen
}

// ---------------------------------------------------------------------------
// Structured run output
// ---------------------------------------------------------------------------

// runReport is what --json writes: everything the soak harness needs to decide
// whether a run drifted, without re-parsing the human-readable report.
type runReport struct {
	StartedUnix  int64   `json:"started_unix"`
	FinishedUnix int64   `json:"finished_unix"`
	DurationSecs float64 `json:"duration_seconds"`
	ControlPlane string  `json:"control_plane"`
	Distribution string  `json:"key_distribution"`
	ZipfS        float64 `json:"zipf_s,omitempty"`
	Replicas     int     `json:"replicas"`
	Workers      int     `json:"workers"`
	ValueSize    int     `json:"value_size_bytes"`
	Keys         int     `json:"keys"`
	ReadRatio    float64 `json:"read_ratio"`
	Seed         int64   `json:"seed"`
	Mode         string  `json:"mode"`

	Operations   int64   `json:"operations"`
	Reads        int64   `json:"reads"`
	Writes       int64   `json:"writes"`
	OpsPerSecond float64 `json:"ops_per_second"`
	Verified     int64   `json:"verified"`
	Hits         int64   `json:"hits"`
	Misses       int64   `json:"misses"`
	Mismatches   int64   `json:"mismatches"`
	RPCErrors    int64   `json:"rpc_errors"`

	// LatencySampled is true when the percentiles below come from a bounded
	// reservoir rather than every operation. Recorded so a reader is never left
	// guessing which one they are looking at.
	LatencySampled bool    `json:"latency_sampled"`
	LatencySamples int     `json:"latency_samples"`
	ReadP50Ms      float64 `json:"read_p50_ms"`
	ReadP99Ms      float64 `json:"read_p99_ms"`
	ReadP999Ms     float64 `json:"read_p999_ms"`
	WriteP50Ms     float64 `json:"write_p50_ms"`
	WriteP99Ms     float64 `json:"write_p99_ms"`
	WriteP999Ms    float64 `json:"write_p999_ms"`

	Concentration concentration    `json:"concentration"`
	Intervals     []intervalReport `json:"intervals,omitempty"`

	// Drift compares the first and last quarter of the interval series. It is
	// the soak's actual question, computed here so every consumer gets the same
	// answer rather than each re-deriving it.
	Drift *driftReport `json:"drift,omitempty"`
}

// driftReport is the "did it get worse over time" comparison.
type driftReport struct {
	Intervals        int     `json:"intervals_compared"`
	FirstQuarterOps  float64 `json:"first_quarter_ops_per_second"`
	LastQuarterOps   float64 `json:"last_quarter_ops_per_second"`
	ThroughputChange float64 `json:"throughput_change_percent"`
	FirstQuarterP99  float64 `json:"first_quarter_read_p99_ms"`
	LastQuarterP99   float64 `json:"last_quarter_read_p99_ms"`
	P99Change        float64 `json:"read_p99_change_percent"`
}

func computeDrift(intervals []intervalReport) *driftReport {
	// Four intervals is the floor at which "first quarter" and "last quarter"
	// are different, non-overlapping windows. Below that, reporting a trend
	// would be inventing one.
	if len(intervals) < 4 {
		return nil
	}
	quarter := len(intervals) / 4
	mean := func(rows []intervalReport, pick func(intervalReport) float64) float64 {
		if len(rows) == 0 {
			return 0
		}
		var sum float64
		for _, row := range rows {
			sum += pick(row)
		}
		return sum / float64(len(rows))
	}
	first := intervals[:quarter]
	last := intervals[len(intervals)-quarter:]

	firstOps := mean(first, func(r intervalReport) float64 { return r.OpsPerSecond })
	lastOps := mean(last, func(r intervalReport) float64 { return r.OpsPerSecond })
	firstP99 := mean(first, func(r intervalReport) float64 { return r.ReadP99Ms })
	lastP99 := mean(last, func(r intervalReport) float64 { return r.ReadP99Ms })

	change := func(from, to float64) float64 {
		if from == 0 {
			return 0
		}
		return 100 * (to - from) / from
	}
	return &driftReport{
		Intervals:        len(intervals),
		FirstQuarterOps:  firstOps,
		LastQuarterOps:   lastOps,
		ThroughputChange: change(firstOps, lastOps),
		FirstQuarterP99:  firstP99,
		LastQuarterP99:   lastP99,
		P99Change:        change(firstP99, lastP99),
	}
}

func writeJSONReport(path string, report runReport) error {
	raw, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("encode run report: %w", err)
	}
	raw = append(raw, '\n')
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		return fmt.Errorf("write run report to %s: %w", path, err)
	}
	return nil
}

// percentileMs is percentile() in the units the JSON report uses.
func percentileMs(sorted []time.Duration, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	return float64(percentile(sorted, p).Nanoseconds()) / 1e6
}
