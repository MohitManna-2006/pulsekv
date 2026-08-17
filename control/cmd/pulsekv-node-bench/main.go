// Command pulsekv-node-bench drives one data-plane node directly.
//
// This is the v2 equivalent of v1's tests/benchmark.c, and it inherits that
// file's evidence rules rather than reinventing them:
//
//   - EVERY read is verified against the value that was written. An unchecked
//     reply is not throughput, it is a number. Values are generated from the
//     key index rather than stored, so a full working set can be verified
//     byte-for-byte without holding a second copy of it in the client.
//   - Warmup is excluded from the measurement, not merely mentioned.
//   - Latency is reported as a distribution -- min/p50/p99/p999/max -- because
//     a mean alone hides exactly the tail that tiering creates.
//   - Reads and writes are reported separately. Averaging a RAM hit with an
//     NVMe promotion produces a number that describes neither.
//
// No cluster routing: this talks to one node, which is the point. It
// establishes the single-node baseline that Phase 9's distributed benchmark
// measures its overhead against.
//
// See deploy/bench-node.sh for the two scenarios the Phase 1 summary records.
package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	nodev1 "pulsekv/control/gen/node/v1"
	"pulsekv/control/internal/transport"
)

const (
	// Matches the node's own ceiling so a large-value run is not rejected by
	// the transport before it reaches the handler.
	maxMessageBytes = 8 * 1024 * 1024
)

func main() {
	var (
		address     = flag.String("address", "127.0.0.1:7100", "node NodeService address")
		concurrency = flag.Int("concurrency", 16, "number of concurrent client workers")
		valueSize   = flag.Int("value-size", 16*1024, "value size in bytes")
		keys        = flag.Int("keys", 2048, "working set size, in keys")
		ops         = flag.Int("ops", 50000, "measured operations (excludes warmup)")
		warmupOps   = flag.Int("warmup-ops", 5000, "operations to run and discard before measuring")
		readRatio   = flag.Float64("read-ratio", 0.8, "fraction of operations that are reads")
		seed        = flag.Int64("seed", 1, "PRNG seed for key selection; the workload is reproducible")
		label       = flag.String("label", "", "scenario name for the report header")
		keyPrefix   = flag.String("key-prefix", "bench", "key namespace, so runs do not collide")
		timeout     = flag.Duration("rpc-timeout", 60*time.Second, "per-RPC deadline")
	)
	flag.Parse()

	if *concurrency < 1 || *keys < 1 || *ops < 1 || *valueSize < 0 {
		fmt.Fprintln(os.Stderr, "pulsekv-node-bench: concurrency, keys and ops must be positive")
		os.Exit(2)
	}
	if *readRatio < 0 || *readRatio > 1 {
		fmt.Fprintln(os.Stderr, "pulsekv-node-bench: --read-ratio must be within [0,1]")
		os.Exit(2)
	}

	b := &bench{
		address:   *address,
		workers:   *concurrency,
		valueSize: *valueSize,
		keys:      *keys,
		ops:       *ops,
		warmup:    *warmupOps,
		readRatio: *readRatio,
		seed:      *seed,
		label:     *label,
		keyPrefix: *keyPrefix,
		timeout:   *timeout,
	}
	if err := b.run(); err != nil {
		fmt.Fprintf(os.Stderr, "pulsekv-node-bench: %v\n", err)
		os.Exit(1)
	}
}

// ---------------------------------------------------------------------------
// deterministic values
// ---------------------------------------------------------------------------

// valueFor regenerates the exact bytes stored under key index i. Same
// xorshift64* the C engine tests use, for the same reason: a few lines, no
// dependency, and bytes no allocator or filesystem will reproduce by accident.
func valueFor(buf []byte, i int) {
	state := uint64(i)*2862933555777941757 + 3037000493
	if state == 0 {
		state = 1
	}
	var scratch [8]byte
	for off := 0; off < len(buf); off += 8 {
		state ^= state >> 12
		state ^= state << 25
		state ^= state >> 27
		binary.LittleEndian.PutUint64(scratch[:], state*0x2545F4914F6CDD1D)
		copy(buf[off:], scratch[:])
	}
}

func (b *bench) keyFor(i int) []byte {
	return []byte(fmt.Sprintf("%s:%08d", b.keyPrefix, i))
}

// ---------------------------------------------------------------------------

type bench struct {
	address   string
	workers   int
	valueSize int
	keys      int
	ops       int
	warmup    int
	readRatio float64
	seed      int64
	label     string
	keyPrefix string
	timeout   time.Duration
}

type sample struct {
	d      time.Duration
	isRead bool
}

type workerResult struct {
	samples    []sample
	reads      int
	writes     int
	hits       int
	misses     int // a miss on a populated key: a correctness failure
	mismatches int // wrong bytes returned: a correctness failure
	errors     int
	firstErr   error
}

func (b *bench) dial() (*grpc.ClientConn, error) {
	return grpc.NewClient(b.address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxMessageBytes),
			grpc.MaxCallSendMsgSize(maxMessageBytes),
		))
}

func (b *bench) run() error {
	// One connection per worker rather than one shared: a single gRPC
	// ClientConn multiplexes onto one HTTP/2 connection, whose stream
	// concurrency limit would become the thing being measured instead of the
	// node.
	conns := make([]*grpc.ClientConn, b.workers)
	clients := make([]nodev1.NodeServiceClient, b.workers)
	for i := 0; i < b.workers; i++ {
		c, err := b.dial()
		if err != nil {
			return fmt.Errorf("dial %s: %w", b.address, err)
		}
		defer c.Close()
		conns[i] = c
		clients[i] = nodev1.NewNodeServiceClient(c)
	}

	b.printHeader()

	// --- capacity before, so the report can show what the run did to the tiers
	capBefore, err := b.capacity(clients[0])
	if err != nil {
		return fmt.Errorf("capacity: %w", err)
	}

	// --- populate
	popStart := time.Now()
	if err := b.populate(clients); err != nil {
		return fmt.Errorf("populate: %w", err)
	}
	popElapsed := time.Since(popStart)
	fmt.Printf("populate      %d keys in %s  (%.0f keys/s, %s/s)\n\n",
		b.keys, popElapsed.Round(time.Millisecond),
		float64(b.keys)/popElapsed.Seconds(),
		humanBytes(int64(float64(b.keys*b.valueSize)/popElapsed.Seconds())))

	// --- warmup, discarded
	if b.warmup > 0 {
		if _, err := b.mixed(clients, b.warmup, false); err != nil {
			return fmt.Errorf("warmup: %w", err)
		}
	}

	// --- measure
	start := time.Now()
	results, err := b.mixed(clients, b.ops, true)
	if err != nil {
		return fmt.Errorf("measure: %w", err)
	}
	elapsed := time.Since(start)

	capAfter, err := b.capacity(clients[0])
	if err != nil {
		return fmt.Errorf("capacity: %w", err)
	}

	return b.report(results, elapsed, capBefore, capAfter)
}

func (b *bench) printHeader() {
	name := b.label
	if name == "" {
		name = "node benchmark"
	}
	fmt.Printf("=== %s ===\n\n", name)
	fmt.Printf("node          %s\n", b.address)
	fmt.Printf("workers       %d\n", b.workers)
	fmt.Printf("value size    %d B\n", b.valueSize)
	fmt.Printf("working set   %d keys (%s)\n", b.keys,
		humanBytes(int64(b.keys)*int64(b.valueSize)))
	fmt.Printf("operations    %d measured, %d warmup (discarded)\n", b.ops, b.warmup)
	fmt.Printf("read ratio    %.2f\n", b.readRatio)
	fmt.Printf("path          %s\n\n", b.pathName())
}

func (b *bench) pathName() string {
	if b.valueSize > transport.UnaryValueLimit {
		return "PutChunked/GetChunked (value exceeds the 4 MiB unary limit)"
	}
	return "unary Put/Get"
}

// populate writes the whole working set, sharded across the workers.
func (b *bench) populate(clients []nodev1.NodeServiceClient) error {
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		firs error
	)
	per := (b.keys + b.workers - 1) / b.workers

	for w := 0; w < b.workers; w++ {
		lo := w * per
		hi := lo + per
		if hi > b.keys {
			hi = b.keys
		}
		if lo >= hi {
			continue
		}
		wg.Add(1)
		go func(client nodev1.NodeServiceClient, lo, hi int) {
			defer wg.Done()
			buf := make([]byte, b.valueSize)
			for i := lo; i < hi; i++ {
				valueFor(buf, i)
				if err := b.put(client, b.keyFor(i), buf); err != nil {
					mu.Lock()
					if firs == nil {
						firs = fmt.Errorf("key %d: %w", i, err)
					}
					mu.Unlock()
					return
				}
			}
		}(clients[w], lo, hi)
	}
	wg.Wait()
	return firs
}

// mixed runs `total` operations spread across the workers.
func (b *bench) mixed(clients []nodev1.NodeServiceClient, total int, record bool) ([]workerResult, error) {
	results := make([]workerResult, b.workers)
	per := (total + b.workers - 1) / b.workers

	var wg sync.WaitGroup
	for w := 0; w < b.workers; w++ {
		n := per
		if w*per+n > total {
			n = total - w*per
		}
		if n <= 0 {
			continue
		}
		wg.Add(1)
		go func(idx int, client nodev1.NodeServiceClient, n int) {
			defer wg.Done()
			r := &results[idx]
			if record {
				r.samples = make([]sample, 0, n)
			}
			// Per-worker PRNG seeded deterministically: the same --seed
			// reproduces the same access pattern, which is what makes two runs
			// comparable.
			rng := rand.New(rand.NewSource(b.seed + int64(idx)*7919))
			buf := make([]byte, b.valueSize)
			expect := make([]byte, b.valueSize)

			for i := 0; i < n; i++ {
				keyIdx := rng.Intn(b.keys)
				isRead := rng.Float64() < b.readRatio

				begin := time.Now()
				var err error
				if isRead {
					r.reads++
					var got []byte
					var found bool
					got, found, err = b.get(client, b.keyFor(keyIdx))
					d := time.Since(begin)
					if err == nil {
						if !found {
							// Every key was populated and nothing is ever
							// dropped when a data-dir is configured, so a miss
							// here is a real correctness failure, not a cache
							// effect.
							r.misses++
						} else {
							r.hits++
							valueFor(expect, keyIdx)
							if len(got) != b.valueSize || !bytesEqual(got, expect) {
								r.mismatches++
							}
						}
					}
					if record && err == nil {
						r.samples = append(r.samples, sample{d: d, isRead: true})
					}
				} else {
					r.writes++
					valueFor(buf, keyIdx)
					err = b.put(client, b.keyFor(keyIdx), buf)
					d := time.Since(begin)
					if record && err == nil {
						r.samples = append(r.samples, sample{d: d, isRead: false})
					}
				}
				if err != nil {
					r.errors++
					if r.firstErr == nil {
						r.firstErr = err
					}
				}
			}
		}(w, clients[w], n)
	}
	wg.Wait()
	return results, nil
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// RPC helpers -- unary below the wire limit, chunked above it
// ---------------------------------------------------------------------------

func (b *bench) put(client nodev1.NodeServiceClient, key, value []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), b.timeout)
	defer cancel()
	return transport.Put(ctx, client, key, value)
}

func (b *bench) get(client nodev1.NodeServiceClient, key []byte) ([]byte, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), b.timeout)
	defer cancel()
	return transport.GetWithMode(ctx, client, key, transport.ReadModeForSize(b.valueSize))
}

func (b *bench) capacity(client nodev1.NodeServiceClient) (*nodev1.CapacityResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), b.timeout)
	defer cancel()
	return client.Capacity(ctx, &nodev1.CapacityRequest{})
}

// ---------------------------------------------------------------------------
// reporting
// ---------------------------------------------------------------------------

func (b *bench) report(results []workerResult, elapsed time.Duration,
	capBefore, capAfter *nodev1.CapacityResponse) error {

	var (
		all, reads, writes           []time.Duration
		nReads, nWrites              int
		hits, misses, mismatch, errs int
		firstErr                     error
	)
	for i := range results {
		r := &results[i]
		nReads += r.reads
		nWrites += r.writes
		hits += r.hits
		misses += r.misses
		mismatch += r.mismatches
		errs += r.errors
		if firstErr == nil {
			firstErr = r.firstErr
		}
		for _, s := range r.samples {
			all = append(all, s.d)
			if s.isRead {
				reads = append(reads, s.d)
			} else {
				writes = append(writes, s.d)
			}
		}
	}

	sortDurations(all)
	sortDurations(reads)
	sortDurations(writes)

	fmt.Printf("%-10s %8s %9s %9s %9s %9s %9s %9s\n",
		"", "count", "min", "p50", "p99", "p999", "max", "mean")
	printRow("read", reads)
	printRow("write", writes)
	printRow("overall", all)

	total := nReads + nWrites
	fmt.Printf("\nthroughput    %.0f ops/s over %s  (%s/s of value payload)\n",
		float64(total)/elapsed.Seconds(), elapsed.Round(time.Millisecond),
		humanBytes(int64(float64(total*b.valueSize)/elapsed.Seconds())))

	fmt.Printf("\nverification  %d reads, %d hits verified byte-for-byte, "+
		"%d mismatches, %d unexpected misses, %d rpc errors\n",
		nReads, hits, mismatch, misses, errs)

	fmt.Printf("capacity      before: %s\n", capString(capBefore))
	fmt.Printf("              after:  %s\n", capString(capAfter))

	nvme := capAfter.GetBytesInNvmeTier()
	ram := capAfter.GetBytesInRamTier()
	if ram+nvme > 0 {
		fmt.Printf("              %.1f%% of value bytes are on the NVMe tier\n",
			100*float64(nvme)/float64(ram+nvme))
	}
	fmt.Println()

	// A benchmark that reports throughput for unverified replies is not
	// evidence. Fail the run rather than printing a number next to a wrong
	// answer.
	if mismatch > 0 || misses > 0 || errs > 0 {
		if firstErr != nil {
			return fmt.Errorf("correctness failure: %d mismatches, %d unexpected misses, "+
				"%d rpc errors (first: %v)", mismatch, misses, errs, firstErr)
		}
		return fmt.Errorf("correctness failure: %d mismatches, %d unexpected misses",
			mismatch, misses)
	}
	return nil
}

func printRow(name string, sorted []time.Duration) {
	if len(sorted) == 0 {
		fmt.Printf("%-10s %8d %9s %9s %9s %9s %9s %9s\n", name, 0, "-", "-", "-", "-", "-", "-")
		return
	}
	var sum time.Duration
	for _, d := range sorted {
		sum += d
	}
	fmt.Printf("%-10s %8d %9s %9s %9s %9s %9s %9s\n",
		name, len(sorted),
		ms(sorted[0]),
		ms(pct(sorted, 0.50)),
		ms(pct(sorted, 0.99)),
		ms(pct(sorted, 0.999)),
		ms(sorted[len(sorted)-1]),
		ms(sum/time.Duration(len(sorted))))
}

func pct(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	// Nearest-rank. With p999 over a few thousand samples the difference from
	// an interpolating estimator is well inside the run-to-run noise, and this
	// one is impossible to get subtly wrong.
	idx := int(p * float64(len(sorted)))
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func ms(d time.Duration) string {
	return fmt.Sprintf("%.3fms", float64(d.Nanoseconds())/1e6)
}

func sortDurations(d []time.Duration) {
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
}

func capString(c *nodev1.CapacityResponse) string {
	return fmt.Sprintf("keys=%d ram=%s nvme=%s",
		c.GetResidentKeys(), humanBytes(int64(c.GetBytesInRamTier())),
		humanBytes(int64(c.GetBytesInNvmeTier())))
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for v := n / unit; v >= unit && exp < 3; v /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGT"[exp])
}
