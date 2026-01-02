.PHONY: test
test:
	go test -v -race

.PHONY: integration-test
integration-test:
	go test -v -tags=integration -count=1 -timeout=10m

.PHONY: test-all
test-all: test integration-test

.PHONY: build
build:
	go build ./...

.PHONY: lint
lint:
	golangci-lint run

.PHONY: tidy
tidy:
	go mod tidy

.PHONY: clean
clean:
	go clean
	rm -rf /tmp/ducklake-test-*

.PHONY: help
help:
	@echo "Available targets:"
	@echo "  test             - Run unit tests"
	@echo "  integration-test - Run integration tests (requires Docker)"
	@echo "  test-all         - Run all tests"
	@echo "  build            - Build the exporter"
	@echo "  lint             - Run linters"
	@echo "  tidy             - Tidy go modules"
	@echo "  clean            - Clean build artifacts and test files"
