// Package promexport writes Prometheus text exposition format.
//
// Deliberately not a Prometheus client library. This project has kept its
// dependency list short on purpose, and the exposition format is a handful of
// lines of text: a HELP line, a TYPE line, and one sample per series. Pulling in
// a registry, collector interfaces, and a HTTP middleware stack to emit that
// would be the largest dependency in the repo, serving the smallest need.
//
// What it does buy, by existing at all rather than being inlined twice, is that
// the exporter and the load generator format identically — a scraper reading
// both sees one convention, not two that drifted.
package promexport

import (
	"fmt"
	"sort"
	"strings"
)

// ContentType is the exposition format's media type, for an HTTP handler.
const ContentType = "text/plain; version=0.0.4; charset=utf-8"

// Writer accumulates one scrape response.
type Writer struct {
	out strings.Builder
}

// Help emits the HELP and TYPE lines that precede a metric family.
//
// kind is "counter", "gauge", "histogram", "summary", or "untyped". Prometheus
// treats an unknown type as untyped rather than failing, but writing the right
// one matters: a counter and a gauge are queried differently, and labelling a
// monotonic total as a gauge invites someone to graph its value instead of its
// rate.
func (w *Writer) Help(name, kind, text string) {
	fmt.Fprintf(&w.out, "# HELP %s %s\n# TYPE %s %s\n", name, escapeHelp(text), name, kind)
}

// Metric emits one sample. Labels are sorted so a diff between two scrapes is
// about the values rather than about map iteration order.
func (w *Writer) Metric(name string, labels map[string]string, value float64) {
	if len(labels) == 0 {
		fmt.Fprintf(&w.out, "%s %g\n", name, value)
		return
	}
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	pairs := make([]string, 0, len(keys))
	for _, key := range keys {
		pairs = append(pairs, fmt.Sprintf("%s=%q", key, escapeLabel(labels[key])))
	}
	fmt.Fprintf(&w.out, "%s{%s} %g\n", name, strings.Join(pairs, ","), value)
}

// String returns the accumulated exposition text.
func (w *Writer) String() string { return w.out.String() }

// With returns base plus one more label, leaving base untouched. Callers build
// a per-target label set once and then vary one dimension off it.
func With(base map[string]string, key, value string) map[string]string {
	out := make(map[string]string, len(base)+1)
	for k, v := range base {
		out[k] = v
	}
	out[key] = value
	return out
}

// Bool renders a boolean as the 1/0 Prometheus has no other way to express.
func Bool(ok bool) float64 {
	if ok {
		return 1
	}
	return 0
}

// escapeHelp keeps a HELP line on one line. Backslash and newline are the only
// characters the format gives meaning to here.
func escapeHelp(text string) string {
	text = strings.ReplaceAll(text, `\`, `\\`)
	return strings.ReplaceAll(text, "\n", `\n`)
}

// escapeLabel escapes a label value, which is quoted and so must also escape
// the quote itself.
func escapeLabel(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `"`, `\"`)
	return strings.ReplaceAll(value, "\n", `\n`)
}
