# Local workflows for the ticket office.
#
# Run `make` with no target for the list.

.DEFAULT_GOAL := help

GO_IMAGE   ?= golang:1.26
LINT_IMAGE ?= golangci/golangci-lint:v2.12.2-alpine
# Docker Desktop installs a `docker` *shell script* next to docker.exe, for the
# benefit of WSL users, and an MSYS shell finds the extensionless script first.
# The script re-execs docker.exe, and MSYS_NO_PATHCONV below does not survive
# that hop, so every path argument is rewritten behind our back: `-w /src`
# becomes `-w C:/Program Files/Git/src`, and `-v host:/src` loses its colon and
# degrades into an anonymous volume — which mounts an *empty* directory and
# fails with "directory prefix . does not contain main module" rather than
# anything that points at the cause. Naming the executable skips the script.
#
# MSYSTEM is the signal rather than OS: it is set by exactly the shells that
# have the script on their PATH, and msys2's make unsets OS before the makefile
# is parsed. Elsewhere — Linux, macOS, PowerShell — plain `docker` is correct.
DOCKER     ?= $(if $(MSYSTEM),docker.exe,docker)
COMPOSE    ?= $(DOCKER) compose
K6         ?= k6
K6_IMAGE   ?= grafana/k6:latest
API_URL    ?= http://localhost:8080
# Compose derives this from the project name in docker-compose.yml.
COMPOSE_NETWORK ?= ticket-office_default
# Virtual users for the campaign load test. Must exceed the campaign size to
# put the stock under real contention.
VUS        ?= 500

# On Windows, make runs recipes through Git's MSYS shell, which helpfully
# rewrites anything that looks like a Unix path into a Windows one before the
# process sees it — turning `-w /src` into `-w C:/Program Files/Git/src` and
# making every docker run below fail. The variable is meaningless on Linux and
# macOS, so exporting it unconditionally costs nothing.
export MSYS_NO_PATHCONV := 1

# The Go toolchain runs in a container by default. Two reasons: local builds
# then compile in the same environment as CI, and on the machine this was
# developed on Windows Smart App Control blocks the unsigned toolchain
# binaries outright. Named volumes keep the module and build caches warm
# between runs, which is the difference between a two second and a two minute
# test cycle.
#
# Set GO_LOCAL=1 to use a host toolchain instead: `make test GO_LOCAL=1`.
DOCKER_RUN = $(DOCKER) run --rm \
	-v "$(CURDIR)":/src -w /src \
	-v ticket-office-gomod:/go/pkg/mod \
	-v ticket-office-gobuild:/root/.cache/go-build

# Integration tests start real containers through Testcontainers. From inside
# the toolchain container that needs three things: the host's Docker socket, a
# route back to the host, and TESTCONTAINERS_HOST_OVERRIDE so the library
# reaches the sibling containers it starts by their published ports on the host
# rather than by an address only the daemon can see.
DOCKER_RUN_TC = $(DOCKER_RUN) \
	-v /var/run/docker.sock:/var/run/docker.sock \
	--add-host host.docker.internal:host-gateway \
	-e TESTCONTAINERS_HOST_OVERRIDE=host.docker.internal

ifdef GO_LOCAL
GO             = go
GO_INTEGRATION = go
LINT           = golangci-lint
else
GO             = $(DOCKER_RUN) $(GO_IMAGE) go
GO_INTEGRATION = $(DOCKER_RUN_TC) $(GO_IMAGE) go
LINT           = $(DOCKER_RUN) -v ticket-office-golangci:/root/.cache/golangci-lint $(LINT_IMAGE) golangci-lint
endif

.PHONY: help
help: ## List the available targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

## --- stack -----------------------------------------------------------------

.PHONY: up
up: ## Build and start the whole stack, waiting until it is healthy
	$(COMPOSE) up -d --build --wait

.PHONY: down
down: ## Stop the stack, keeping volumes
	$(COMPOSE) down

.PHONY: logs
logs: ## Follow the logs of every service
	$(COMPOSE) logs -f

.PHONY: ps
ps: ## Show the state of every service
	$(COMPOSE) ps

.PHONY: reset
reset: ## Destroy all state and bring the stack back up clean
	$(COMPOSE) down -v
	$(COMPOSE) up -d --build --wait

## --- code ------------------------------------------------------------------

.PHONY: build
build: ## Compile every package
	$(GO) build ./...

.PHONY: test
test: ## Run the unit tests with the race detector
	$(GO) test -race -shuffle=on ./...

.PHONY: integration-test
integration-test: ## Run the integration tests against real containers
	$(GO_INTEGRATION) test -race -shuffle=on -tags=integration -timeout 600s ./...

.PHONY: cover
cover: ## Run the tests and print total coverage
	$(GO) test -race -covermode=atomic -coverprofile=coverage.out ./...
	$(GO) tool cover -func=coverage.out | tail -n 1

.PHONY: lint
lint: ## Run golangci-lint
	$(LINT) run

.PHONY: fmt
fmt: ## Format every Go file
	$(GO) fmt ./...

.PHONY: tidy
tidy: ## Tidy go.mod and go.sum
	$(GO) mod tidy

.PHONY: verify
verify: fmt tidy build lint test ## Everything CI runs, in the same order

## --- load testing ----------------------------------------------------------

.PHONY: load-test
load-test: ## Run the k6 smoke test against a running stack
	$(K6) run -e BASE_URL=$(API_URL) loadtest/smoke.js

.PHONY: load-test-campaign
load-test-campaign: ## Campaign load test from the host (use after `make reset`)
	$(K6) run -e BASE_URL=$(API_URL) -e VUS=$(VUS) loadtest/campaign.js

.PHONY: load-test-campaign-internal
load-test-campaign-internal: ## Campaign load test from inside the docker network
	# On Docker Desktop for Windows the published-port proxy refuses a large
	# share of connections in a burst — at 500 virtual users it dropped 57% of
	# them before the API saw anything. Running the generator on the same
	# network measures the API instead of the host's port forwarding.
	$(DOCKER) run --rm --network $(COMPOSE_NETWORK) \
		-v "$(CURDIR)/loadtest":/loadtest \
		-e BASE_URL=http://api:8080 -e VUS=$(VUS) \
		$(K6_IMAGE) run /loadtest/campaign.js
