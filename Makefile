.PHONY: help compile test vendor vendor-sync clean-vendor build clean all init gencert

help: ## Show help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  make %-15s %s\n", $$1, $$2}'

all: vendor compile test build ## Run all steps: vendor, compile, test, build

compile: ## Generate proto code
	protoc api/v1/*.proto \
		--go_out=. \
		--go-grpc_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative \
		--proto_path=.

test: ## Run all tests
	go test -v ./...

test-run: ## Run specific test e.g. make test-run T=TestIndex P=./internal/log
	go test -v --race -count=1 -run $(T) $(P)
