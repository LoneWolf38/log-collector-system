.PHONY: generate run build

generate:
	go test -v -run TestGenerate

run:
	cd goapp && go run ./cmd/server

build:
	cd goapp && go build -o bin/server ./cmd/server
