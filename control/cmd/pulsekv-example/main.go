// Command pulsekv-example is a deliberately non-LLM use of the public SDK: a
// tiny note store backed by a live PulseKV cluster.
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"time"

	"pulsekv/control/pkg/client"
)

func main() {
	controlPlane := flag.String("control-plane", "127.0.0.1:7000", "ClusterMetadataService address")
	namespace := flag.String("namespace", "example-notes", "note key namespace")
	timeout := flag.Duration("timeout", 10*time.Second, "deadline for the example")
	flag.Parse()

	if err := run(*controlPlane, *namespace, *timeout); err != nil {
		fmt.Fprintf(os.Stderr, "pulsekv-example: %v\n", err)
		os.Exit(1)
	}
}

func run(controlPlane, namespace string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	c, err := client.New(controlPlane)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer c.Close()

	notes := map[string][]byte{
		fmt.Sprintf("%s:groceries", namespace): []byte("oats, coffee, oranges"),
		fmt.Sprintf("%s:project", namespace):   []byte("ship the routing skeleton"),
		fmt.Sprintf("%s:weekend", namespace):   []byte("walk the High Line"),
	}

	keys := make([]string, 0, len(notes))
	for key := range notes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := c.Put(ctx, []byte(key), notes[key]); err != nil {
			return fmt.Errorf("store note %q: %w", key, err)
		}
	}

	for _, key := range keys {
		value, found, err := c.Get(ctx, []byte(key))
		if err != nil {
			return fmt.Errorf("read note %q: %w", key, err)
		}
		if !found {
			return fmt.Errorf("read note %q: unexpected miss", key)
		}
		if !bytes.Equal(value, notes[key]) {
			return fmt.Errorf("read note %q: value mismatch", key)
		}
		fmt.Printf("%s = %s\n", key, value)
	}

	matches, err := c.PrefixMatch(ctx, []byte(namespace+":"))
	if err != nil {
		return fmt.Errorf("list notes: %w", err)
	}
	if len(matches) != len(notes) {
		return fmt.Errorf("list notes: got %d matches, want %d", len(matches), len(notes))
	}
	fmt.Printf("verified %d notes through the public PulseKV client\n", len(matches))
	return nil
}
