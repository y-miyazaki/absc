.PHONY: build test lint fmt vet vuln clean help

# Binary
BINARY := absc
CMD    := ./cmd/absc

build: ## Build the binary
	go build -trimpath -o $(BINARY) $(CMD)

test: ## Run tests with race detector and coverage
	mkdir -p coverage
	CGO_ENABLED=1 go test ./... -race -count=1 -covermode=atomic -coverprofile=coverage/coverage.out
	go tool cover -func=coverage/coverage.out

lint: ## Run golangci-lint
	golangci-lint run --config=.golangci.yaml

fmt: ## Run gofumpt
	gofumpt -w .

vet: ## Run go vet
	go vet ./...

vuln: ## Run govulncheck
	govulncheck ./...

clean: ## Remove build artifacts
	rm -f $(BINARY)
	rm -rf coverage dist

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'
