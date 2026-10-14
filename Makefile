.PHONY: all proto server start feeder pilot test tidy

all: tidy proto

proto:
	@echo "Generating protobuf Go code..."
	mkdir -p gen/loan/v1
	protoc -I proto/keceku/loan/v1 \
		--go_out=gen/loan/v1 --go_opt=paths=source_relative \
		--go-grpc_out=gen/loan/v1 --go-grpc_opt=paths=source_relative \
		proto/keceku/loan/v1/loan_prewarm.proto

server:
	go run main.go start

start:
	go run main.go start

feeder:
	go run main.go feeder --count=100000 --chunk-size=500

pilot:
	go run main.go pilot --count=100000 --chunk-size=500

test:
	go test -v -race ./...

tidy:
	go mod tidy
