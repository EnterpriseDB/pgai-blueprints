# Core Banking Fraud Audit Stack
#
# For demonstration purposes only.
#
# Combines ML fraud detection with AI-powered fraud audit

.PHONY: start stop clean logs health ui-app ui-langflow ui-jupyter ui-minio train-model setup-langflow

# === LIFECYCLE ===

start:
	@./scripts/start-stack.sh up -d
	@echo ""
	@echo "Stack starting... Services will be ready in ~2-3 minutes"
	@echo ""
	@echo "Bank App:    http://localhost:3001"
	@echo "LangFlow:    http://localhost:7861"
	@echo "Jupyter:     http://localhost:8889/?token=databox"
	@echo "MinIO:       http://localhost:9004"
	@echo "ClickHouse:  http://localhost:8125/play"

start-no-creds:
	docker compose up -d
	@echo "Started without AWS credential export (use for debugging)"

stop:
	docker compose stop

clean:
	docker compose down -v
	@echo "All containers and volumes removed"

restart:
	docker compose restart

# === MONITORING ===

logs:
	docker compose logs -f

logs-app:
	docker compose logs -f app

logs-langflow:
	docker compose logs -f langflow

logs-ml:
	docker compose logs -f ml-inference-kafka ml-inference-pgaa ml-inference-risingwave ml-inference-clickhouse

logs-pgd:
	docker compose logs -f pgd

health:
	@echo "=== Service Health ==="
	@docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"

# === UI SHORTCUTS ===

ui-app:
	open http://localhost:3001

ui-langflow:
	open http://localhost:7861

ui-jupyter:
	open http://localhost:8889/?token=databox

ui-minio:
	open http://localhost:9004

ui-clickhouse:
	open http://localhost:8125/play

# === ML MODEL ===

train-model:
	@echo "Training fraud detection ML model with MLOps pipeline..."
	docker exec bfsi-jupyter bash -c 'jupyter nbconvert --to notebook --execute /home/jovyan/notebooks/mlflow-mlops-pipeline.ipynb --ExecutePreprocessor.timeout=600'
	@echo "Model training complete! View in MLflow: http://localhost:5001/#/models"

check-models:
	@echo "=== ML Models ==="
	docker exec bfsi-jupyter ls -la /models/*.pkl 2>/dev/null || echo "No models found yet"

# === FRAUD AUDIT ===

view-fraud-rules:
	docker exec bfsi-pgd psql -U postgres -d demo -c \
		"SELECT rule_id, rule_name, region, vendor, action FROM fraud_rules ORDER BY region, rule_id;"

view-hitl-queue:
	docker exec bfsi-pgd psql -U postgres -d demo -c \
		"SELECT * FROM v_pending_hitl LIMIT 20;"

view-audit-summary:
	docker exec bfsi-pgd psql -U postgres -d demo -c \
		"SELECT * FROM v_fraud_audit_summary ORDER BY hour DESC LIMIT 10;"

# === DATABASE ===

psql:
	docker exec -it bfsi-pgd psql -U postgres -d demo

clickhouse-client:
	docker exec -it bfsi-clickhouse clickhouse-client --user default --password admin123

# === LANGFLOW ===

setup-langflow:
	@echo "LangFlow is available at http://localhost:7861"
	@echo ""
	@echo "To create a fraud audit agent:"
	@echo "1. Open LangFlow UI"
	@echo "2. Create new flow"
	@echo "3. Add components: Chat Input, Agent, Postgres Query Tool, Fraud Rules Search"
	@echo "4. Connect: Chat Input -> Agent -> Chat Output"
	@echo "5. Add tools to Agent: Postgres Query Tool, Fraud Rules Search"

# === HELP ===

help:
	@echo "Core Banking Fraud Audit Stack"
	@echo ""
	@echo "Lifecycle:"
	@echo "  make start          - Start all services"
	@echo "  make stop           - Stop all services"
	@echo "  make clean          - Remove all containers and volumes"
	@echo "  make restart        - Restart all services"
	@echo ""
	@echo "Monitoring:"
	@echo "  make logs           - Follow all logs"
	@echo "  make logs-ml        - Follow ML inference logs"
	@echo "  make health         - Show service health"
	@echo ""
	@echo "UI Shortcuts:"
	@echo "  make ui-app         - Open Bank App"
	@echo "  make ui-langflow    - Open LangFlow"
	@echo "  make ui-jupyter     - Open Jupyter"
	@echo ""
	@echo "ML Model:"
	@echo "  make train-model    - Train fraud detection model"
	@echo "  make check-models   - List trained models"
	@echo ""
	@echo "Fraud Audit:"
	@echo "  make view-fraud-rules   - Show fraud detection rules"
	@echo "  make view-hitl-queue    - Show pending HITL approvals"
	@echo "  make view-audit-summary - Show audit summary"
	@echo ""
	@echo "Database:"
	@echo "  make psql           - Connect to PostgreSQL"
	@echo "  make clickhouse-client - Connect to ClickHouse"
