// Module path is `pulsekv/control` so that the generated packages land on the
// import paths the .proto files declare, e.g.
//   option go_package = "pulsekv/control/gen/node/v1;nodev1"
// resolves to control/gen/node/v1 inside this module.
module pulsekv/control

go 1.25.0

require (
	google.golang.org/grpc v1.83.0
	google.golang.org/protobuf v1.36.12
	gopkg.in/yaml.v3 v3.0.1
)

require (
	golang.org/x/net v0.55.0 // indirect
	golang.org/x/sys v0.45.0 // indirect
	golang.org/x/text v0.37.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260526163538-3dc84a4a5aaa // indirect
)
