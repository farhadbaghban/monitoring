SHELL := /bin/bash
COMPOSE := docker compose
GO_IMAGE := golang:1.25
GO_RUN := docker run --rm -v "$(CURDIR)":/src -w /src -e GOTOOLCHAIN=auto -e GOPROXY $(GO_IMAGE)

.PHONY: help up down logs build test test-unit test-null smoke load chaos migrate seed-year fmt vet deps ps

help:
	@echo "make up        - build and start the whole stack"
	@echo "make down      - stop the stack"
	@echo "make test      - run Go unit tests in the golang:1.25 container"
	@echo "make test-unit - run only tests under internal/ in the Go container"
	@echo "make test-null - run unit tests and the sensor NULL integration test in Compose"
	@echo "make smoke     - verify the pipeline and a one-minute Kafka outage"
	@echo "make chaos     - verify Kafka, ingest, WAL-full, and database recovery"
	@echo "make load      - run the load test and produce graphs"
	@echo "make migrate   - apply SQL migrations to the running DB"
	@echo "make seed-year - insert one year of chart test data for device 90"
	@echo "make logs      - tail logs"

deps:
	$(GO_RUN) go mod tidy

build:
	$(GO_RUN) go build -mod=readonly ./...

test:
	$(GO_RUN) go test -mod=readonly ./... -count=1

test-unit:
	$(GO_RUN) go test -mod=readonly ./internal/... -count=1

test-null:
	bash scripts/compose-test.sh null

fmt:
	$(GO_RUN) bash -ec 'gofmt -w $$(find . -name "*.go" -type f)'

vet:
	$(GO_RUN) go vet -mod=readonly ./...

up:
	$(COMPOSE) build api nginx
	$(COMPOSE) up -d --no-build --wait --wait-timeout 240

down:
	$(COMPOSE) down --remove-orphans

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f --tail=100

migrate:
	$(COMPOSE) run --rm migrate

seed-year:
	$(COMPOSE) build api
	$(COMPOSE) run --no-deps --rm seed-year

smoke:
	bash scripts/compose-test.sh smoke

chaos:
	bash scripts/compose-test.sh chaos

load:
	bash scripts/compose-test.sh load

