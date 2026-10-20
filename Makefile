VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
PEERS   := n1=127.0.0.1:7001,n2=127.0.0.1:7002,n3=127.0.0.1:7003

.PHONY: build test test-short race bench lint cluster stop-cluster docker clean

build: ## Build quorumkv and qkv into ./bin
	go build -trimpath -ldflags="-X main.version=$(VERSION)" -o bin/ ./cmd/...

test: ## Full test suite with the race detector (fault injection, linearizability)
	go test -race -count=1 ./...

test-short: ## Quick test run, skipping the longest scenarios
	go test -short -count=1 ./...

bench: build ## Benchmark a local 3-node cluster (start it with `make cluster`)
	./bin/qkv bench -n 30000 -c 64 -size 128 -writes 1
	./bin/qkv bench -n 30000 -c 32 -size 128 -writes 0.5

lint:
	gofmt -l . | (! grep .) && go vet ./...
	golangci-lint run

cluster: build ## Run a local 3-node cluster in the background (logs in ./data)
	@mkdir -p data
	@for i in 1 2 3; do \
		./bin/quorumkv -id n$$i -peers $(PEERS) -data-dir data/n$$i > data/n$$i.log 2>&1 & echo $$! > data/n$$i.pid; \
	done
	@sleep 1 && ./bin/qkv status

stop-cluster:
	@for i in 1 2 3; do [ -f data/n$$i.pid ] && kill $$(cat data/n$$i.pid) 2>/dev/null; rm -f data/n$$i.pid; done; true

docker:
	docker build --build-arg VERSION=$(VERSION) -t quorumkv:$(VERSION) .

clean: stop-cluster
	rm -rf bin data
