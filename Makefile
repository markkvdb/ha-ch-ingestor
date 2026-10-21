.PHONY: build test lint fmt image image-slim up down logs query clean

build:
	cargo build --release

test:
	cargo test

lint:
	cargo clippy --all-targets -- -D warnings
	cargo clippy --all-targets --features mock -- -D warnings

fmt:
	cargo fmt

image:
	docker build -t ha-ch-ingestor:dev .

image-slim:
	docker build --build-arg FEATURES=none -t ha-ch-ingestor:slim .

up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f ingestor

query:
	curl -s http://localhost:8123/ --data-binary \
		"SELECT entity_id, count(), max(state_float) FROM homeassistant.states GROUP BY entity_id ORDER BY entity_id FORMAT PrettyCompact"

clean:
	cargo clean
