package main

// The sustained (duration-bounded) run, which is what Phase 9.4's soak drives.
//
// Its job is different from the fixed-count benchmark's. A benchmark answers
// "how fast is this"; a soak answers "did anything get worse while nothing was
// supposed to change". So this path reports a time series rather than one
// aggregate, and it computes the first-quarter-vs-last-quarter comparison
// itself so every consumer reads the same answer instead of eyeballing a table.

import (
	"fmt"
	"sync"
	"time"

	sdk "pulsekv/control/pkg/client"
)

func (b *benchmark) runSustained(clients []*sdk.Client) error {
	b.live = &liveCounters{}
	b.buckets = make([]*intervalBucket, b.workers)
	for i := range b.buckets {
		b.buckets[i] = &intervalBucket{}
	}
	b.sampler = newReservoir(b.latencySamples, b.seed+7)

	deadline := time.Now().Add(b.duration)
	started := time.Now()
	b.measureStarted = started

	fmt.Printf("sustained     %s at a %s reporting interval; latency percentiles from a "+
		"%d-sample reservoir\n", b.duration, b.reportInterval(), b.latencySamples)
	if b.continueOnError {
		fmt.Printf("errors        counted and survived (--continue-on-error); value mismatches still stop the run\n")
	}
	fmt.Println()
	fmt.Printf("%8s %10s %10s %10s %10s %10s %8s %8s %8s\n",
		"elapsed", "ops/s", "rd p50", "rd p99", "wr p50", "wr p99", "verified", "misses", "errors")

	results := make([]workerResult, b.workers)
	var wg sync.WaitGroup
	for worker := 0; worker < b.workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			b.runWorker(clients[worker%len(clients)], worker, &results[worker],
				b.seed+1_000_003, false,
				// Checking the clock every operation rather than every N is
				// deliberate: an operation here is a network round trip, so a
				// time.Now() next to it is free, and the run then stops when it
				// was asked to rather than one batch later.
				func(int) bool { return time.Now().Before(deadline) })
		}(worker)
	}

	done := make(chan struct{})
	intervals := make(chan []intervalReport, 1)
	go func() { intervals <- b.reportIntervals(started, done) }()

	wg.Wait()
	close(done)
	series := <-intervals
	elapsed := time.Since(started)

	summary := summarize(results)
	return b.finishSustained(summary, series, elapsed)
}

// reportIntervals drains every worker's bucket on each tick and prints one row.
func (b *benchmark) reportIntervals(started time.Time, done <-chan struct{}) []intervalReport {
	ticker := time.NewTicker(b.reportInterval())
	defer ticker.Stop()

	var series []intervalReport
	var last struct{ reads, writes, verified, misses, mismatches, errors int64 }
	previous := started

	for {
		select {
		case <-done:
			return series
		case now := <-ticker.C:
			var reads, writes []time.Duration
			for _, bucket := range b.buckets {
				r, w := bucket.drain()
				reads = append(reads, r...)
				writes = append(writes, w...)
			}
			sortDurations(reads)
			sortDurations(writes)

			readsNow := b.live.reads.Load()
			writesNow := b.live.writes.Load()
			verifiedNow := b.live.verified.Load()
			missesNow := b.live.misses.Load()
			mismatchesNow := b.live.mismatches.Load()
			errorsNow := b.live.rpcErrors.Load()

			window := now.Sub(previous).Seconds()
			operations := (readsNow - last.reads) + (writesNow - last.writes)
			row := intervalReport{
				Index:        len(series) + 1,
				ElapsedSecs:  now.Sub(started).Seconds(),
				Operations:   operations,
				Reads:        readsNow - last.reads,
				Writes:       writesNow - last.writes,
				OpsPerSecond: float64(operations) / window,
				ReadP50Ms:    percentileMs(reads, 0.50),
				ReadP99Ms:    percentileMs(reads, 0.99),
				WriteP50Ms:   percentileMs(writes, 0.50),
				WriteP99Ms:   percentileMs(writes, 0.99),
				Verified:     verifiedNow - last.verified,
				Misses:       missesNow - last.misses,
				Mismatches:   mismatchesNow - last.mismatches,
				RPCErrors:    errorsNow - last.errors,
			}
			series = append(series, row)

			fmt.Printf("%7.0fs %10.0f %10s %10s %10s %10s %8d %8d %8d\n",
				row.ElapsedSecs, row.OpsPerSecond,
				fmt.Sprintf("%.2fms", row.ReadP50Ms), fmt.Sprintf("%.2fms", row.ReadP99Ms),
				fmt.Sprintf("%.2fms", row.WriteP50Ms), fmt.Sprintf("%.2fms", row.WriteP99Ms),
				row.Verified, row.Misses, row.RPCErrors)

			last.reads, last.writes = readsNow, writesNow
			last.verified, last.misses = verifiedNow, missesNow
			last.mismatches, last.errors = mismatchesNow, errorsNow
			previous = now
		}
	}
}

func (b *benchmark) finishSustained(summary resultSummary, series []intervalReport,
	elapsed time.Duration) error {

	samples, seen := b.sampler.snapshot()
	var reads, writes []time.Duration
	for _, sample := range samples {
		if sample.isRead {
			reads = append(reads, sample.duration)
		} else {
			writes = append(writes, sample.duration)
		}
	}
	sortDurations(reads)
	sortDurations(writes)

	total := int64(summary.nReads + summary.nWrites)
	fmt.Println()
	fmt.Printf("%-10s %8s %9s %9s %9s %9s\n", "", "sampled", "p50", "p99", "p999", "max")
	printSampledRow("read", reads)
	printSampledRow("write", writes)
	// Only call it a sample when it actually is one. A run short enough to fit
	// inside the reservoir kept every operation, and saying otherwise would
	// understate the numbers' precision as surely as the reverse would overstate
	// it.
	if int64(len(samples)) < seen {
		fmt.Printf("\nlatency       a uniform %d-sample reservoir drawn from %d operations\n",
			len(samples), seen)
	} else {
		fmt.Printf("\nlatency       every one of %d operations; the %d-sample reservoir did not fill\n",
			seen, b.latencySamples)
	}
	fmt.Printf("throughput    %.0f ops/s over %s\n",
		float64(total)/elapsed.Seconds(), elapsed.Round(time.Second))
	fmt.Printf("verification  %d reads, %d hits, %d verified byte-for-byte, "+
		"%d mismatches, %d misses, %d RPC errors\n",
		summary.nReads, summary.hits, summary.verified,
		summary.mismatches, summary.misses, summary.rpcErrors)

	drift := computeDrift(series)
	if drift != nil {
		fmt.Printf("\ndrift         first quarter vs last quarter of %d intervals\n", drift.Intervals)
		fmt.Printf("              throughput %.0f -> %.0f ops/s (%+.1f%%)\n",
			drift.FirstQuarterOps, drift.LastQuarterOps, drift.ThroughputChange)
		fmt.Printf("              read p99   %.2f -> %.2f ms (%+.1f%%)\n",
			drift.FirstQuarterP99, drift.LastQuarterP99, drift.P99Change)
	} else {
		fmt.Printf("\ndrift         not computed: %d interval(s) is too few to compare a first "+
			"and last quarter\n", len(series))
	}

	if b.distribution == distributionZipf {
		b.reportConcentration()
	}

	if b.jsonPath != "" {
		report := b.baseReport(elapsed)
		report.Mode = "duration"
		report.Operations = total
		report.Reads = int64(summary.nReads)
		report.Writes = int64(summary.nWrites)
		report.OpsPerSecond = float64(total) / elapsed.Seconds()
		report.Verified = int64(summary.verified)
		report.Hits = int64(summary.hits)
		report.Misses = int64(summary.misses)
		report.Mismatches = int64(summary.mismatches)
		report.RPCErrors = int64(summary.rpcErrors)
		report.LatencySampled = int64(len(samples)) < seen
		report.LatencySamples = len(samples)
		report.ReadP50Ms = percentileMs(reads, 0.50)
		report.ReadP99Ms = percentileMs(reads, 0.99)
		report.ReadP999Ms = percentileMs(reads, 0.999)
		report.WriteP50Ms = percentileMs(writes, 0.50)
		report.WriteP99Ms = percentileMs(writes, 0.99)
		report.WriteP999Ms = percentileMs(writes, 0.999)
		report.Intervals = series
		report.Drift = drift
		if err := writeJSONReport(b.jsonPath, report); err != nil {
			return err
		}
		fmt.Printf("\nreport        %s\n", b.jsonPath)
	}

	// A sustained run's pass condition differs from a fixed one's on purpose.
	// It cannot require an exact operation count, because the count is whatever
	// the clock allowed; and under --continue-on-error it must not fail on the
	// transient errors that deliberate fault injection creates. What it must
	// never tolerate is a value mismatch, or a read that came back and went
	// unverified.
	if summary.mismatches > 0 {
		return fmt.Errorf("sustained run correctness failure: %d value mismatch(es) over %d reads "+
			"(first: %v)", summary.mismatches, summary.nReads, summary.firstErr)
	}
	// Every hit must have been verified. A hit that was not compared is not
	// evidence of anything, and letting one through would quietly turn this
	// into a throughput number with no correctness claim attached.
	if summary.verified != summary.hits {
		return fmt.Errorf("sustained run accounting failure: %d hits but only %d verified",
			summary.hits, summary.verified)
	}
	if !b.continueOnError && summary.rpcErrors > 0 {
		return fmt.Errorf("sustained run had %d RPC error(s) without --continue-on-error (first: %v)",
			summary.rpcErrors, summary.firstErr)
	}
	if total == 0 {
		return fmt.Errorf("sustained run completed no operations")
	}
	return nil
}

func printSampledRow(name string, sorted []time.Duration) {
	if len(sorted) == 0 {
		fmt.Printf("%-10s %8d %9s %9s %9s %9s\n", name, 0, "-", "-", "-", "-")
		return
	}
	fmt.Printf("%-10s %8d %9s %9s %9s %9s\n",
		name, len(sorted),
		milliseconds(percentile(sorted, 0.50)),
		milliseconds(percentile(sorted, 0.99)),
		milliseconds(percentile(sorted, 0.999)),
		milliseconds(sorted[len(sorted)-1]))
}

// baseReport fills the configuration half of a run report.
func (b *benchmark) baseReport(elapsed time.Duration) runReport {
	report := runReport{
		FinishedUnix: time.Now().Unix(),
		DurationSecs: elapsed.Seconds(),
		ControlPlane: b.controlPlane,
		Distribution: b.distribution,
		Replicas:     b.replicas,
		Workers:      b.workers,
		ValueSize:    b.valueSize,
		Keys:         b.keys,
		ReadRatio:    b.readRatio,
		Seed:         b.seed,
	}
	report.StartedUnix = report.FinishedUnix - int64(elapsed.Seconds())
	if b.distribution == distributionZipf {
		report.ZipfS = b.zipfS
		report.Concentration = measureConcentration(b.keyCounts, 1)
	}
	return report
}

// writeFixedReport is the --json output for the fixed-operation-count path.
func (b *benchmark) writeFixedReport(summary resultSummary, elapsed time.Duration) error {
	report := b.baseReport(elapsed)
	report.Mode = "operations"
	total := int64(summary.nReads + summary.nWrites)
	report.Operations = total
	report.Reads = int64(summary.nReads)
	report.Writes = int64(summary.nWrites)
	report.OpsPerSecond = float64(total) / elapsed.Seconds()
	report.Verified = int64(summary.verified)
	report.Hits = int64(summary.hits)
	report.Misses = int64(summary.misses)
	report.Mismatches = int64(summary.mismatches)
	report.RPCErrors = int64(summary.rpcErrors)
	report.LatencySampled = false
	report.LatencySamples = len(summary.all)
	report.ReadP50Ms = percentileMs(summary.reads, 0.50)
	report.ReadP99Ms = percentileMs(summary.reads, 0.99)
	report.ReadP999Ms = percentileMs(summary.reads, 0.999)
	report.WriteP50Ms = percentileMs(summary.writes, 0.50)
	report.WriteP99Ms = percentileMs(summary.writes, 0.99)
	report.WriteP999Ms = percentileMs(summary.writes, 0.999)
	if err := writeJSONReport(b.jsonPath, report); err != nil {
		return err
	}
	fmt.Printf("report        %s\n", b.jsonPath)
	return nil
}

// reportConcentration prints the realized traffic shape.
func (b *benchmark) reportConcentration() {
	if b.keyCounts == nil {
		return
	}
	for _, topPercent := range []float64{1, 5, 10} {
		c := measureConcentration(b.keyCounts, topPercent)
		fmt.Printf("skew          hottest %g%% of keys took %.1f%% of operations "+
			"(%d of %d keys touched)\n",
			c.TopPercent, c.ShareOfOps, c.DistinctKeys, b.keys)
	}
}

// ensureKeyCounts allocates the per-key access tally when the run needs it.
func (b *benchmark) ensureKeyCounts() {
	if b.distribution == distributionZipf && b.keyCounts == nil {
		b.keyCounts = make([]int64, b.keys)
	}
}
