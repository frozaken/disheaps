module test_client

go 1.21

require (
	github.com/disheap/disheap v0.0.0
	google.golang.org/grpc v1.57.0
)

replace github.com/disheap/disheap => ../

require (
	github.com/golang/protobuf v1.5.3 // indirect
	golang.org/x/net v0.19.0 // indirect
	golang.org/x/sys v0.15.0 // indirect
	golang.org/x/text v0.14.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20231212172506-995d672761c0 // indirect
	google.golang.org/protobuf v1.31.0 // indirect
)
