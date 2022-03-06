rebuild:
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		repository_server.proto

install-tools:
	brew install protobuf
	brew install protoc-gen-go
	brew install protoc-gen-go-grpc

