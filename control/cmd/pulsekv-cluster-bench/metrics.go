package main

// The load generator's own metrics endpoint.
//
// Cache hit rate is a client-side fact. The exporter's canary probe writes a
// key and reads it straight back, so its hit rate is a health signal, not a
// cache hit rate — it is 100% whenever the system works at all. What a hit rate
// actually means is "of the reads this workload issued, how many found the
// key", and only the thing issuing the reads knows that.
//
// So the generator exposes it, live, while a run is in progress. During Phase
// 9.4's soak that is the metric that shows a fault window as it happens: misses
// climb while a node is down and its shards are moving, and return to zero when
// the topology settles. A final tally at the end of a four-hour run cannot show
// that shape at all.

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"pulsekv/control/internal/promexport"
)

// serveMetrics starts the generator's metrics endpoint, if one was configured.
//
// It never returns an error that stops a run: this is instrumentation, and a
// benchmark that refuses to measure anything because its metrics port was taken
// would be trading the result for the observation of it.
func (b *benchmark) serveMetrics() {
	if b.metricsListen == "" {
		return
	}
	listener, err := net.Listen("tcp", b.metricsListen)
	if err != nil {
		log.Printf("metrics endpoint disabled: listen on %s: %v", b.metricsListen, err)
		return
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", b.writeMetrics)
	go func() { _ = http.Serve(listener, mux) }()
	fmt.Printf("metrics       http://%s/metrics\n", listener.Addr())
}

func (b *benchmark) writeMetrics(w http.ResponseWriter, _ *http.Request) {
	out := &promexport.Writer{}
	labels := map[string]string{
		"key_distribution": b.distribution,
		"key_prefix":       b.keyPrefix,
	}

	out.Help("pulsekv_workload_running", "gauge",
		"1 while the load generator is executing its measured phase.")
	out.Metric("pulsekv_workload_running", labels, promexport.Bool(b.live != nil))

	out.Help("pulsekv_workload_elapsed_seconds", "gauge",
		"Seconds since the measured phase started.")
	if !b.measureStarted.IsZero() {
		out.Metric("pulsekv_workload_elapsed_seconds", labels, time.Since(b.measureStarted).Seconds())
	}

	if b.live == nil {
		_, _ = w.Write([]byte(out.String()))
		return
	}

	reads := b.live.reads.Load()
	writes := b.live.writes.Load()
	hits := b.live.hits.Load()
	misses := b.live.misses.Load()

	out.Help("pulsekv_workload_operations_total", "counter",
		"Operations this generator has issued, by kind.")
	out.Metric("pulsekv_workload_operations_total", promexport.With(labels, "kind", "read"), float64(reads))
	out.Metric("pulsekv_workload_operations_total", promexport.With(labels, "kind", "write"), float64(writes))

	out.Help("pulsekv_workload_reads_total", "counter",
		"Read outcomes. hit/(hit+miss) is the cache hit rate this workload actually saw. "+
			"A mismatch is a read that returned the wrong bytes and is never tolerated.")
	out.Metric("pulsekv_workload_reads_total", promexport.With(labels, "result", "hit"), float64(hits))
	out.Metric("pulsekv_workload_reads_total", promexport.With(labels, "result", "miss"), float64(misses))
	out.Metric("pulsekv_workload_reads_total", promexport.With(labels, "result", "mismatch"),
		float64(b.live.mismatches.Load()))

	out.Help("pulsekv_workload_verified_total", "counter",
		"Reads compared byte-for-byte against the value written for that key. Every hit must "+
			"appear here; a hit that was not verified is not evidence of anything.")
	out.Metric("pulsekv_workload_verified_total", labels, float64(b.live.verified.Load()))

	out.Help("pulsekv_workload_rpc_errors_total", "counter",
		"Operations that failed outright. Expected to be nonzero while a fault is being "+
			"injected and a topology change is in flight.")
	out.Metric("pulsekv_workload_rpc_errors_total", labels, float64(b.live.rpcErrors.Load()))

	out.Help("pulsekv_workload_hit_rate", "gauge",
		"hits / (hits + misses) over the run so far. Reported directly as well as via the "+
			"counters above, because it is the number a reader is looking for.")
	if total := hits + misses; total > 0 {
		out.Metric("pulsekv_workload_hit_rate", labels, float64(hits)/float64(total))
	}

	_, _ = w.Write([]byte(out.String()))
}
