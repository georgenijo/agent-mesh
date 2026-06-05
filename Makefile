.PHONY: build vet test tidy fmt clean

build:
	go build -o bin/meshd ./cmd/meshd
	go build -o bin/mesh  ./cmd/mesh

vet:
	go vet ./...

test:
	go test ./...

tidy:
	go mod tidy

fmt:
	gofmt -l -w .

clean:
	rm -rf bin
