.PHONY: all up down build-agents run-agents run-orchestrator run-api test clean

all: up build-agents

up:
	docker-compose -f docker/docker-compose.yml up -d

down:
	docker-compose -f docker/docker-compose.yml down

build-agents:
	cd agent_go/collector && go build -o collector .
	cd agent_go/analyzer && go build -o analyzer .
	cd agent_go/alerter && go build -o alerter .
	cd agent_go/recovery && go build -o recovery .

run-agents:
	cd agent_go/collector && ./collector &
	cd agent_go/analyzer && ./analyzer &
	cd agent_go/alerter && ./alerter &
	cd agent_go/recovery && ./recovery &

run-orchestrator:
	cd orchestrator && python orchestrator.py

run-api:
	cd orchestrator && uvicorn api:app --reload --port 8000

test:
	cd orchestrator && pytest test_orchestrator.py -v

clean:
	pkill -f "collector|analyzer|alerter|recovery" || true
	rm -f agent_go/*/collector agent_go/*/analyzer agent_go/*/alerter agent_go/*/recovery

logs:
	docker-compose -f docker/docker-compose.yml logs -f
