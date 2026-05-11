.PHONY: generate run build

generate:
	go run generate.go

run:
	cd goapp && go run ./cmd/server

build:
	cd goapp && go build -o bin/server ./cmd/server
