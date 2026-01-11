PB_REL = "https://github.com/protocolbuffers/protobuf/releases"
install-protoc:
	curl -LO ${PB_REL}/download/v30.2/protoc-30.2-linux-x86_64.zip
	unzip -o protoc-30.2-linux-x86_64.zip -d $${HOME}/.local
	rm protoc-30.2-linux-x86_64.zip

install-go-tools:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.11
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.0

install-cli-tools: install-protoc install-go-tools

grpc:
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative protos/razpravljalnica.proto

test:
	go test -p 1 ./...

build-razcli:
	CGO_ENABLED=0 go build -o bin/razcli cmd/razcli/main.go

run-node-1:
	go run cmd/razpravljalnica-node/main.go --node-id node1 --listen :50001 --control-plane=:50050

run-node-2:
	go run cmd/razpravljalnica-node/main.go --node-id node2 --listen :50002 --control-plane=:50051

run-node-3:
	go run cmd/razpravljalnica-node/main.go --node-id node3 --listen :50003 --control-plane=:50052

run-control-plane:
	go run cmd/razpravljalnica-control-plane/main.go --chain node1,node2,node3

run-gui:
	go run cmd/razpravljalnica-gui-client/main.go --control-plane=localhost:50050

reset-control-planes:
	rm -r raft-data

run-control-plane-raft-0:
	go run cmd/razpravljalnica-control-plane/main.go --listen=:50050 --raft-enabled --raft-node-id=control0 --raft-bind=localhost:50150 --raft-bootstrap --chain node1,node2,node3

run-control-plane-raft-0-rejoin:
	go run cmd/razpravljalnica-control-plane/main.go --listen=:50050 --raft-enabled --raft-node-id=control0 --raft-bind=localhost:50150 --raft-join=:50051

run-control-plane-raft-1:
	go run cmd/razpravljalnica-control-plane/main.go --listen=:50051 --raft-enabled --raft-node-id=control1 --raft-bind=localhost:50151 --raft-join=:50050

run-control-plane-raft-2:
	go run cmd/razpravljalnica-control-plane/main.go --listen=:50052 --raft-enabled --raft-node-id=control2 --raft-bind=localhost:50152 --raft-join=:50051
