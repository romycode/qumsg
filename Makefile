# Entorno de desarrollo local. `make` sin argumentos lista los objetivos.

PKGS     := ./... ./valkey/... ./postgres/...
INTEGRATION_PKGS := ./valkey/... ./postgres/...
GOFILES  := *.go memory valkey postgres
export VALKEY_ADDR ?= localhost:$(or $(VALKEY_PORT),6379)
export POSTGRES_URL ?= postgres://qumsg:qumsg@localhost:$(or $(POSTGRES_PORT),5432)/qumsg?sslmode=disable

# Credenciales de gh solo para los comandos que descargan el repo privado,
# sin tocar la configuración global de git.
GOPRIVATE_ENV := GOWORK=off GOPRIVATE='github.com/romycode/*' GIT_TERMINAL_PROMPT=0 \
	GIT_CONFIG_COUNT=3 \
	GIT_CONFIG_KEY_0=credential.helper GIT_CONFIG_VALUE_0= \
	GIT_CONFIG_KEY_1=credential.helper GIT_CONFIG_VALUE_1='!gh auth git-credential' \
	GIT_CONFIG_KEY_2=url.https://github.com/.insteadOf GIT_CONFIG_VALUE_2=git@github.com:

.DEFAULT_GOAL := help

.PHONY: help
help: ## Muestra esta ayuda
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z_-]+:.*## / {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

## Servicios

.PHONY: up
up: ## Levanta Valkey y PostgreSQL y espera a que estén sanos
	docker compose up -d --wait valkey postgres

.PHONY: down
down: ## Para y elimina los contenedores
	docker compose down

## Tests

.PHONY: test
test: ## Tests unitarios de todos los módulos, con -race
	go test -race $(PKGS)

.PHONY: test-integration
test-integration: up ## Tests de integración de Valkey y PostgreSQL
	go test -race -tags integration $(INTEGRATION_PKGS)

.PHONY: test-valkey
test-valkey: ## Tests de integración de Valkey
	docker compose up -d --wait valkey
	go test -race -tags integration ./valkey/...

.PHONY: test-postgres
test-postgres: ## Tests de integración de PostgreSQL
	docker compose up -d --wait postgres
	go test -race -tags integration ./postgres/...

.PHONY: test-all
test-all: test test-integration ## Unitarios e integración

.PHONY: cover
cover: up ## Cobertura de todos los paquetes, integración incluida
	go test -race -cover $(PKGS)
	go test -race -cover -tags integration $(INTEGRATION_PKGS)

.PHONY: bench
bench: ## Benchmarks en Docker con recursos limitados (BENCH, BENCH_CPUS, BENCH_MEMORY, BENCH_COUNT)
	docker compose run --rm bench

## Calidad

.PHONY: fmt
fmt: ## Formatea el código
	gofmt -w $(GOFILES)

.PHONY: lint
lint: ## gofmt sin cambios y go vet, con y sin el tag integration
	@files="$$(gofmt -l $(GOFILES))"; if [ -n "$$files" ]; then echo "Sin formatear:"; echo "$$files"; exit 1; fi
	go vet $(PKGS)
	go vet -tags integration $(INTEGRATION_PKGS)

.PHONY: check
check: lint test-all ## Todo lo necesario antes de un commit

## Módulos

.PHONY: tidy
tidy: ## go mod tidy en los tres módulos (anidados contra el núcleo publicado)
	go mod tidy
	cd valkey && $(GOPRIVATE_ENV) go mod tidy
	cd postgres && $(GOPRIVATE_ENV) go mod tidy

.PHONY: clean
clean: ## Elimina contenedores, volúmenes y la caché de tests
	docker compose down -v
	go clean -testcache
