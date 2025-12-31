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