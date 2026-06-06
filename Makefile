.PHONY: build vet test test-race e2e fmt fmt-check tidy clean ci

build:
	go build -o bin/meshd ./cmd/meshd
	go build -o bin/mesh  ./cmd/mesh

vet:
	go vet ./...

# Unit + cross-process e2e (test/e2e builds real binaries and boots real
# daemons; ~3s).
test:
	go test ./...

test-race:
	go test -race ./internal/...

e2e:
	go test -count=1 -v ./test/e2e/

fmt:
	gofmt -l -w .

fmt-check:
	@unformatted=$$(gofmt -l .); \
	if [ -n "$$unformatted" ]; then \
		echo "gofmt needed on:"; echo "$$unformatted"; exit 1; \
	fi

tidy:
	go mod tidy

# Mirrors CI: .github/workflows/ci.yml
ci: fmt-check build vet test

clean:
	rm -rf bin
