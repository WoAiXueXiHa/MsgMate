CONFIG ?= config/config-local.toml

.PHONY: msgsvr run clean docker-run docker-stop test integration
msgsvr:
	go build -o bin/msgmate ./src
run:
	go run ./src -config "$(CONFIG)"
clean:
	rm -f bin/msgmate
docker-run:
	docker compose up -d mysql redis zookeeper kafka
docker-stop:
	docker compose down
test:
	go test ./...
	go test -race ./...
	go vet ./...
integration:
	MSGMATE_TEST_CONFIG="$(CONFIG)" go test -race -tags integration ./src/ctrl/consumer -v -count=1
