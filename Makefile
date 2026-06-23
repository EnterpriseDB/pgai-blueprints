.PHONY: help setup agent stop restart stop-all clean status logs logs-follow

.DEFAULT_GOAL := help

# Platform detection: Windows (MSYS/MinGW/Git Bash) vs Unix
ifeq ($(OS),Windows_NT)
    IS_WINDOWS := 1
    PYTHON := python
    PROJECT_ROOT := $(CURDIR)
else
    IS_WINDOWS := 0
    PYTHON := python3
    PROJECT_ROOT := $(CURDIR)
endif

help:
	@echo ""
	@echo "  EDB Postgres® AI Blueprints - Available Commands"
	@echo "  ========================================="
	@echo ""
	@echo "  Getting Started:"
	@echo "    make setup        Check Docker, install Python deps, verify ports"
	@echo "    make agent        Start the chat agent at http://127.0.0.1:4000"
	@echo ""
	@echo "  Agent Control:"
	@echo "    make stop         Stop the agent (containers keep running)"
	@echo "    make restart      Stop and restart the agent"
	@echo ""
	@echo "  Container Control:"
	@echo "    make stop-all     Stop all containers (keeps data volumes)"
	@echo "    make clean        Stop everything + remove volumes/networks/ports across infras"
	@echo "                      (sweeps both Docker Desktop and Colima; stops Colima if it"
	@echo "                       holds a diab port; refuses to kill foreign processes unless"
	@echo "                       FORCE_KILL_PORTS=1 is set)"
	@echo ""
	@echo "  Info:"
	@echo "    make status       Show running containers, projects, port usage"
	@echo "    make logs         Show last 50 lines of agent log"
	@echo "    make logs-follow  Tail agent log in real time"
	@echo "    make help         Show this help message"
	@echo ""

setup:
	@chmod +x bootstrap.sh
	@bash bootstrap.sh

# Platform-specific agent target
ifeq ($(IS_WINDOWS),1)
agent:
	@if curl -s --connect-timeout 1 http://127.0.0.1:4000/ >/dev/null 2>&1; then \
		echo "Agent already running on port 4000. Run 'make stop' first."; \
	else \
		bash "$(PROJECT_ROOT)/scripts/start-agent-windows.sh" >/dev/null; \
		sleep 5; \
		if curl -s --connect-timeout 2 http://127.0.0.1:4000/ >/dev/null 2>&1; then \
			echo "Agent started at http://127.0.0.1:4000"; \
			echo "Logs: make logs | make logs-follow"; \
		else \
			echo "Agent failed to start. Check: make logs"; \
		fi; \
	fi
else
agent:
	@mkdir -p "$(PROJECT_ROOT)/engine/agent/logs"
	@port_in_use() { ss -tlnp 2>/dev/null | grep -q ":4000 " || { lsof -ti:4000 >/dev/null 2>&1; }; }; \
	if port_in_use; then \
		echo "Agent already running on port 4000. Run 'make stop' first."; \
	else \
		if [ -f "$(PROJECT_ROOT)/.env" ]; then set -a; . "$(PROJECT_ROOT)/.env"; set +a; fi; \
		if [ -n "$$AWS_PROFILE" ] && [ -z "$$AWS_ACCESS_KEY_ID" ]; then \
			echo "Exporting AWS credentials from profile: $$AWS_PROFILE"; \
			eval "$$(aws configure export-credentials --profile $$AWS_PROFILE --format env 2>/dev/null)" || \
				echo "WARNING: Could not export AWS credentials. Run: aws sso login --profile $$AWS_PROFILE"; \
		fi; \
		export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN; \
		cd "$(PROJECT_ROOT)/engine/agent" && nohup $(PYTHON) app.py > logs/agent.log 2>&1 & \
		echo "$$!" > "$(PROJECT_ROOT)/engine/agent/logs/agent.pid"; \
		for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30; do \
			sleep 1; \
			if ss -tlnp 2>/dev/null | grep -q ":4000 " || lsof -ti:4000 >/dev/null 2>&1; then \
				echo "Agent started at http://127.0.0.1:4000 (PID: $$(cat "$(PROJECT_ROOT)/engine/agent/logs/agent.pid"))"; \
				echo "Logs: make logs | make logs-follow"; \
				break; \
			fi; \
			if [ "$$i" = "30" ]; then \
				echo "Agent failed to start. Check: make logs"; \
			fi; \
		done; \
	fi
endif

# Platform-specific stop target
ifeq ($(IS_WINDOWS),1)
stop:
	@echo "Stopping agent..."
	@if [ -f "$(PROJECT_ROOT)/engine/agent/logs/agent.pid" ]; then \
		pid=$$(cat "$(PROJECT_ROOT)/engine/agent/logs/agent.pid"); \
		taskkill //F //PID $$pid 2>/dev/null || true; \
		rm -f "$(PROJECT_ROOT)/engine/agent/logs/agent.pid"; \
	fi
	@for pid in $$(netstat -ano 2>/dev/null | grep ":4000.*LISTENING" | awk '{print $$5}' | sort -u); do \
		taskkill //F //PID $$pid 2>/dev/null || true; \
	done
	@echo "Agent stopped."
else
stop:
	@echo "Stopping agent..."
	@if [ -f "$(PROJECT_ROOT)/engine/agent/logs/agent.pid" ]; then \
		kill $$(cat "$(PROJECT_ROOT)/engine/agent/logs/agent.pid") 2>/dev/null || true; \
		rm -f "$(PROJECT_ROOT)/engine/agent/logs/agent.pid"; \
	fi
	@{ lsof -ti:4000 2>/dev/null || ss -tlnp 2>/dev/null | grep ':4000 ' | sed -n 's/.*pid=\([0-9][0-9]*\).*/\1/p'; } | xargs kill -9 2>/dev/null || true
	@echo "Agent stopped."
endif

restart: stop
	@echo "Restarting agent..."
	@sleep 1
	@$(MAKE) agent

stop-all: stop
	@found=0; \
	for dir in "$(PROJECT_ROOT)/stacks"/*/ "$(PROJECT_ROOT)/plugins"/*/; do \
		if [ -f "$$dir/docker-compose.yaml" ] && [ "$$(basename $$dir)" != "_template" ]; then \
			has_containers=$$(cd "$$dir" && docker compose ps -aq 2>/dev/null | wc -l | tr -d ' '); \
			if [ "$$has_containers" != "0" ]; then \
				echo "  Stopping $$(basename $$dir) ($$has_containers containers)..."; \
				cd "$$dir" && PROFS=$$(docker compose config --profiles 2>/dev/null | awk '{printf " --profile %s", $$0}') && \
					eval "docker compose $$PROFS kill" 2>/dev/null || true; \
					eval "docker compose $$PROFS down --remove-orphans -t 1" 2>/dev/null || true; \
				found=1; \
			fi; \
		fi; \
	done; \
	if [ "$$found" = "0" ]; then \
		echo "No running containers to stop."; \
	else \
		echo "All containers stopped."; \
	fi

clean: stop
	@echo "============================================"
	@echo "  Cleaning up all resources..."
	@echo "============================================"
	@echo ""
	@echo "[1/5] Cleaning containers + volumes on active runtime..."
	@found=0; \
	for dir in "$(PROJECT_ROOT)/stacks"/*/ "$(PROJECT_ROOT)/plugins"/*/; do \
		if [ -f "$$dir/docker-compose.yaml" ] && [ "$$(basename $$dir)" != "_template" ]; then \
			has_containers=$$(cd "$$dir" && docker compose ps -aq 2>/dev/null | wc -l | tr -d ' '); \
			if [ "$$has_containers" != "0" ]; then \
				echo "  Cleaning $$(basename $$dir) ($$has_containers containers)..."; \
				cd "$$dir" && PROFS=$$(docker compose config --profiles 2>/dev/null | awk '{printf " --profile %s", $$0}') && \
					eval "docker compose $$PROFS kill" 2>/dev/null || true; \
					eval "docker compose $$PROFS down -v --remove-orphans -t 1" 2>/dev/null || true; \
				found=1; \
			fi; \
		fi; \
	done; \
	if [ "$$found" = "0" ]; then echo "  No containers to clean on active runtime."; fi
	@echo ""
	@echo "[2/5] Cleaning containers on inactive runtime (cross-infra)..."
	@bash "$(PROJECT_ROOT)/scripts/cross-runtime-clean.sh" || true
	@echo ""
	@echo "[3/5] Removing orphan containers (active runtime)..."
	@for prefix in rta- lab- cb- bfsi- bfd- uai- bench- cdc-rw- eapi-rw- kafka-rw- wh-rw- dbox- sovereign- tpl- pg-expense- diab-toolbox-; do \
		docker ps -aq --filter "name=$$prefix" 2>/dev/null | xargs -r docker rm -f 2>/dev/null || true; \
	done
	@echo ""
	@echo "[4/5] Removing project networks..."
	@for net in rta-net cb-network bfsi-network bfd-network uai-network peerdb_network app-net; do \
		docker network rm "$$net" 2>/dev/null || true; \
	done
	@echo ""
	@echo "[5/5] Sweeping host ports for leftover holders..."
	@bash "$(PROJECT_ROOT)/scripts/clean-ports.sh" || true
	@echo ""
	@echo "============================================"
	@echo "  Clean complete. All resources released."
	@echo "============================================"

# Platform-specific status target
ifeq ($(IS_WINDOWS),1)
status:
	@echo "=== Running Containers ==="
	@docker ps --filter 'label=com.docker.compose.project' --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}' 2>/dev/null || echo "No containers running"
	@echo ""
	@echo "=== Compose Projects ==="
	@docker compose ls 2>/dev/null || echo "No compose projects"
	@echo ""
	@echo "=== Port Usage (framework ports) ==="
	@for port in 3000 4000 4566 5050 5432 5433 5435 5436 5691 8080 8081 8085 8123 9000 9001 9301 9400; do \
		if netstat -ano 2>/dev/null | grep ":$$port.*LISTENING" >/dev/null; then \
			echo "  :$$port in use"; \
		fi; \
	done || true
else
status:
	@echo "=== Running Containers ==="
	@docker ps --filter 'label=com.docker.compose.project' --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}' 2>/dev/null || echo "No containers running"
	@echo ""
	@echo "=== Compose Projects ==="
	@docker compose ls 2>/dev/null || echo "No compose projects"
	@echo ""
	@echo "=== Port Usage (framework ports) ==="
	@for port in 3000 4000 4566 5050 5432 5433 5435 5436 5691 8080 8081 8085 8123 9000 9001 9301 9400; do \
		pid=$$(lsof -ti :$$port 2>/dev/null); \
		if [ -n "$$pid" ]; then \
			echo "  :$$port in use (PID: $$pid)"; \
		fi; \
	done || true
endif

logs:
	@if [ -f "$(PROJECT_ROOT)/engine/agent/logs/agent.log" ]; then \
		echo "=== Last 50 lines of agent.log ==="; \
		tail -50 "$(PROJECT_ROOT)/engine/agent/logs/agent.log"; \
	else \
		echo "No log file found. Run 'make agent' first."; \
	fi

logs-follow:
	@if [ -f "$(PROJECT_ROOT)/engine/agent/logs/agent.log" ]; then \
		tail -f "$(PROJECT_ROOT)/engine/agent/logs/agent.log"; \
	else \
		echo "No log file found. Run 'make agent' first."; \
	fi
