# WirePay Orchestrator - Top-level Makefile
# Thomas Wire <tom@thomaswire.com>

SHELL := /bin/bash
.SHELLFLAGS := -euo pipefail -c

.DEFAULT_GOAL := help

BINARY_DIR := bin
DOCKER_REGISTRY := localhost:5000
DOCKER_NS := wpo

SERVICES := wire-order wire-inventory wire-payment wire-notification wire-ledger

GO ?= go
K6 ?= k6
DOCKER ?= docker
KUBECTL ?= kubectl
COMPOSE := $(DOCKER) compose

# Common env vars for service builds
COMMON_GOFLAGS := -trimpath -ldflags="-s -w"

.PHONY: help
help: ## Show this help message
	@echo "WirePay Orchestrator (WPO)"
	@echo "========================="
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-35s\033[0m %s\n", $$1, $$2}'

# Build all services
.PHONY: build-all
build-all: $(addprefix build-,$(SERVICES)) ## Build all service binaries

.PHONY: build-%
build-%: ## Build a specific service (make build-wire-order)
	@echo "Building $*"
	$(GO) build $(COMMON_GOFLAGS) -o $(BINARY_DIR)/$* ./cmd/$*/

# Run a single service locally
.PHONY: run
run: ## Run a single service locally (make run SERVICE=wire-order)
ifndef SERVICE
	$(error SERVICE= is required: make run SERVICE=wire-order)
endif
	cd cmd/$(SERVICE) && $(GO) run ./main.go

# Test all services
.PHONY: test-all
test-all: ## Run tests for all services
	@for svc in $(SERVICES); do \
		echo "Testing $${svc}..."; \
		$(GO) test -race -count=1 -timeout=60s ./cmd/$${svc}/... ; \
		if [ $$? -ne 0 ]; then exit 1; fi; \
	done

.PHONY: test-%
test-%: ## Test a specific service (make test SERVICE=wire-inventory)
ifndef SERVICE
	$(error SERVICE= is required: make test SERVICE=wire-order)
endif
	cd cmd/$(SERVICE) && $(GO) test -race -count=1 ./...

# Lint all services
.PHONY: lint-all
lint-all: ## Run golangci-lint on all services
	@for svc in $(SERVICES); do \
		echo "Linting $${svc}..."; \
		$(GO) vet ./cmd/$${svc}/... || exit 1; \
	done

# Docker Compose management
.PHONY: docker-compose-up
docker-compose-up: ## Start full local dev stack (Kafka, PG, Redis, all services)
	$(COMPOSE) -f deploy/docker-compose.yml up -d --build

.PHONY: docker-compose-down
docker-compose-down: ## Tear down full local dev stack
	$(COMPOSE) -f deploy/docker-compose.yml down -v

.PHONY: docker-compose-logs
docker-compose-logs: ## Follow logs from all services
	$(COMPOSE) -f deploy/docker-compose.yml logs -f --tail=100

# Init databases with migrations
.PHONY: init-dbs
init-dbs: ## Initialize all service databases
	@for svc in $(SERVICES); do \
		echo "Initializing database for $${svc}..."; \
		BASE_DIR=$$(cd cmd/$${svc} && pwd -P); \
		if [ -d "$${BASE_DIR}/migrations" ]; then \
			for m in $$(ls $${BASE_DIR}/migrations/*.sql | sort); do \
				echo "  Applying $$m"; \
				psql -h localhost -p 54320 -U wpo -d "$${svc}_db" -f "$$m" || true; \
			done; \
		fi; \
	done

.PHONY: migrate
migrate: ## Run migrations for a service (make migrate SERVICE=wire-order)
ifndef SERVICE
	$(error SERVICE= is required: make migrate SERVICE=wire-order)
endif
	@echo "Running migrations for $(SERVICE)..."
	@BASE_DIR=$$(cd cmd/$(SERVICE) && pwd -P); \
	for m in $$(ls $$BASE_DIR/migrations/*.sql 2>/dev/null | sort); do \
		echo "  Applying $$m"; \
		psql -h localhost -p 54320 -U wpo -d "$(SERVICE)_db" -f "$$m" || true; \
	done

# Load testing with k6
.PHONY: load-test-flash-sale
load-test-flash-sale: ## Run flash sale load test (k6)
ifndef K6
	$(error k6 is required: install via brew install k6 or go install github.com/grafana/k6@latest)
endif
	$(K6) run --vus=500 --duration=60s load-tests/k6/flash-sale.js

.PHONY: load-test-baseline
load-test-baseline: ## Run baseline load test against all services (k6)
ifndef K6
	$(error k6 is required)
endif
	$(K6) run --vus=50 --duration=120s load-tests/k6/baseline.js

# Chaos engineering
.PHONY: chaos-partition-payment-kafka
chaos-partition-payment-kafka: ## Apply Toxiproxy partition between wire-payment and Kafka
	@echo "Applying network partition via Toxiproxy..."
	@cat load-tests/chaos/toxiproxy-config.json | jq -r '.proxies[] | select(.name=="wire-payment-to-kafka") | .to' || echo "Config check"

.PHONY: chaos-timeout-order-inventory
chaos-timeout-order-inventory: ## Apply latency injection on wire-order → wire-inventory
	@echo "Applying 5s latency injection via Toxiproxy..."
	@cat load-tests/chaos/toxiproxy-config.json | jq -r '.proxies[] | select(.name=="wire-order-to-inventory") | .to' || echo "Config check"

# K8s manifests
.PHONY: k8s-apply-all
k8s-apply-all: ## Apply all WPO manifests to cluster
	$(KUBECTL) apply -f k8s-manifests/base/namespace.yaml
	@for svc in $(SERVICES); do \
		$(KUBECTL) apply -k k8s-manifests/$${svc}; \
	done

.PHONY: k8s-delete-all
k8s-delete-all: ## Delete all WPO manifests from cluster
	@for svc in $(SERVICES); do \
		$(KUBECTL) delete -k k8s-manifests/$${svc} --ignore-not-found; \
	done
	$(KUBECTL) delete -f k8s-manifests/base/namespace.yaml --ignore-not-found

# Tag Docker images locally
.PHONY: tag-all
tag-all: ## Tag all service images as local
	@for svc in $(SERVICES); do \
		$(DOCKER) tag wirepay-orchestrator-$${svc} $(DOCKER_REGISTRY)/wpo/$${svc}:local; \
	done

# Cleanup
.PHONY: clean
clean: ## Remove build artifacts and binaries
	rm -rf $(BINARY_DIR)
	$(DOCKER) rmi -f $(addprefix $(DOCKER_REGISTRY)/$(DOCKER_NS)/,$(SERVICES):latest 2>/dev/null) || true
