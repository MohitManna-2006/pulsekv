package membership

import (
	"strings"
	"testing"
)

func TestNodeMetaRoundTrip(t *testing.T) {
	tests := []NodeMeta{
		{Version: NodeMetaVersion, Role: RoleControl},
		{Version: NodeMetaVersion, Role: RoleData, NodeID: "node-0", Address: "127.0.0.1:7100"},
		{Version: NodeMetaVersion, Role: RoleData, NodeID: "node-v6", Address: "[::1]:7101"},
		{Version: NodeMetaVersion, Role: RoleData, NodeID: "node-dns", Address: "node.local:7102"},
	}
	for _, want := range tests {
		raw, err := EncodeNodeMeta(want)
		if err != nil {
			t.Fatalf("EncodeNodeMeta(%+v): %v", want, err)
		}
		got, err := DecodeNodeMeta(raw)
		if err != nil {
			t.Fatalf("DecodeNodeMeta(%s): %v", raw, err)
		}
		if got != want {
			t.Fatalf("round trip = %+v, want %+v", got, want)
		}
	}
}

func TestDecodeNodeMetaAllowsUnknownFieldsAtKnownVersion(t *testing.T) {
	raw := []byte(`{"version":1,"role":"data","node_id":"node-0","address":"127.0.0.1:7100","future":"ok"}`)
	got, err := DecodeNodeMeta(raw)
	if err != nil {
		t.Fatalf("DecodeNodeMeta: %v", err)
	}
	if got.NodeID != "node-0" || got.Address != "127.0.0.1:7100" {
		t.Fatalf("decoded metadata = %+v", got)
	}
}

func TestNodeMetaRejectsMalformedValues(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{"empty", ``, "empty"},
		{"json", `{`, "decode"},
		{"version", `{"version":2,"role":"control"}`, "unsupported"},
		{"role", `{"version":1,"role":"storage"}`, "unknown"},
		{"data id", `{"version":1,"role":"data","address":"127.0.0.1:7100"}`, "node_id"},
		{"id whitespace", `{"version":1,"role":"data","node_id":" node-0","address":"127.0.0.1:7100"}`, "whitespace"},
		{"id delimiter", `{"version":1,"role":"data","node_id":"node,other","address":"127.0.0.1:7100"}`, "must match"},
		{"data address", `{"version":1,"role":"data","node_id":"node-0"}`, "address"},
		{"address shape", `{"version":1,"role":"data","node_id":"node-0","address":"localhost"}`, "host:port"},
		{"empty host", `{"version":1,"role":"data","node_id":"node-0","address":":7100"}`, "host"},
		{"zero port", `{"version":1,"role":"data","node_id":"node-0","address":"127.0.0.1:0"}`, "1..65535"},
		{"large port", `{"version":1,"role":"data","node_id":"node-0","address":"127.0.0.1:70000"}`, "1..65535"},
		{"control fields", `{"version":1,"role":"control","node_id":"oops"}`, "must not include"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeNodeMeta([]byte(tc.raw))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("DecodeNodeMeta error = %v, want substring %q", err, tc.want)
			}
		})
	}
}

func TestEncodeNodeMetaEnforcesMemberlistLimit(t *testing.T) {
	_, err := EncodeNodeMeta(NodeMeta{
		Version: NodeMetaVersion,
		Role:    RoleData,
		NodeID:  strings.Repeat("n", 600),
		Address: "127.0.0.1:7100",
	})
	if err == nil || !strings.Contains(err.Error(), "limit") {
		t.Fatalf("EncodeNodeMeta error = %v, want memberlist limit", err)
	}
}
