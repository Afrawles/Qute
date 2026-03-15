.DEFAULT_GOAL := help

CONFIG_PATH=${HOME}/.qute

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

test: $(CONFIG_PATH)/policy.csv $(CONFIG_PATH)/model.conf ## Run all tests
	go test -v ./...

test-run: ## Run specific test e.g. make test-run T=TestIndex P=./internal/log
	go test -v --race -count=1 -run $(T) $(P)

init: ## initial cert path
	mkdir -p ${CONFIG_PATH}

gencert: init ## genermate certs
	cfssl gencert \
		-initca test/ca-csr.json | cfssljson -bare ca
	
	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=test/ca-config.json \
		-profile=server \
		test/server-csr.json | cfssljson -bare server

	# cfssl gencert \
	# 	-ca=ca.pem \
	# 	-ca-key=ca-key.pem \
	# 	-config=test/ca-config.json \
	# 	-profile=client \
	# 	test/client-csr.json | cfssljson -bare client

	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=test/ca-config.json \
		-profile=client \
		-cn=root \
		test/client-csr.json | cfssljson -bare root-client

	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=test/ca-config.json \
		-profile=client \
		-cn="nobody" \
		test/client-csr.json | cfssljson -bare nobody-client
	
	mv *.pem *.csr ${CONFIG_PATH}

$(CONFIG_PATH)/model.conf:
	cp test/model.conf $(CONFIG_PATH)/model.conf

$(CONFIG_PATH)/policy.csv:
	cp test/policy.csv $(CONFIG_PATH)/policy.csv
