# Core Banking Fraud Audit

*For demonstration purposes only.*

Real-time ML fraud detection with AI-powered fraud audit using LangFlow.

## Overview

This stack combines:
- **Core Banking Simulator**: Real-time transaction simulation with 4 ML fraud detection paths
- **AI Fraud Audit**: LangFlow visual agent builder for fraud investigation, HITL workflows, and rule-based explanations

## Quick Start

```bash
cd stacks/bfsi-fraud-detection

# Start all services
make start

# Open Bank App
open http://localhost:3001

# Open LangFlow Agent Builder
open http://localhost:7861
```

## Services (18 Containers)

| Service | Port | Description |
|---------|------|-------------|
| Bank App | 3001 | Core banking UI with Fraud Audit tab |
| LangFlow | 7861 | Visual AI agent builder |
| PGD | 7434 | EDB Postgres Distributed (OLTP + PGAA + AIDB) |
| ClickHouse | 8125 | OLAP analytics |
| RisingWave | 4574/5697 | Streaming SQL |
| Kafka | 9096 | Event streaming |
| Kafka Connect | 8084 | Debezium CDC |
| MinIO | 9003/9004 | S3 storage (Iceberg + fraud docs) |
| Lakekeeper | 8181 | Iceberg REST Catalog |
| Jupyter | 8889 | ML model training |

## ML Fraud Detection Paths

| Path | Engine | TTDF | Architecture |
|------|--------|------|--------------|
| 1 | Kafka | 30-80ms | Event-driven, Welford's algorithm |
| 2 | PGAA | 1400-2000ms | WAL sync to Iceberg, Seafowl query |
| 3 | RisingWave | 3000-4500ms | CDC to streaming MVs |
| 4 | ClickHouse | 5000-10000ms | CDC to Kafka Engine |

## Fraud Audit Features

### LangFlow Agent Builder
- Visual drag-and-drop flow creation
- Custom components for fraud audit:
  - **Postgres Query Tool**: Execute SQL queries against PGD
  - **Fraud Rules Search**: Search fraud detection rules by region/vendor
  - **HITL Router**: Route transactions for human approval

### Fraud Rules (15 rules)
- US: Stripe, PayPal, Square (4 rules)
- UK: Stripe, PayPal (2 rules)
- Canada: Stripe, PayPal (2 rules)
- Germany: Stripe, Square (2 rules)
- France: Stripe, PayPal (2 rules)
- Global: High-value, fraud score, patterns (3 rules)

### HITL Workflow
- Transactions with fraud probability > 0.7 or amount > $10,000 trigger HITL
- Approval queue in Bank App Fraud Audit tab
- Approve, reject, or escalate decisions

## Bank App Tabs

1. **OLTP**: Transaction dashboard, live feed
2. **Search**: Query transactions
3. **Fraud Detection**: ML inference metrics, TTDF comparison
4. **Analytics**: ClickHouse/RisingWave analytics
5. **Comparison**: Streaming vs batch comparison
6. **Kafka CDC**: CDC pipeline monitoring
7. **Fraud Audit**: HITL queue, fraud rules, LangFlow integration

## Make Commands

```bash
make start            # Start all services
make stop             # Stop all services
make clean            # Remove containers and volumes
make logs             # Follow all logs
make health           # Show service health

make ui-app           # Open Bank App
make ui-langflow      # Open LangFlow
make ui-jupyter       # Open Jupyter

make train-model      # Train ML fraud detection model
make view-fraud-rules # Show fraud detection rules
make view-hitl-queue  # Show pending HITL approvals
```

## Credentials

| Service | Username | Password |
|---------|----------|----------|
| PGD | postgres | secret |
| ClickHouse | default | admin123 |
| Jupyter | token | databox |
| MinIO | minioadmin | minioadmin123 |
| LangFlow | (auto-login) | - |

## Architecture

```
Transaction → PGD (OLTP)
                ↓
         ┌──────┴──────┐
         ↓             ↓
    Debezium      PGAA (WAL)
         ↓             ↓
       Kafka       Iceberg
         ↓             ↓
    ┌────┴────┐    ML-PGAA
    ↓         ↓
ClickHouse  RisingWave
    ↓         ↓
  ML-CH     ML-RW
    └────┬────┘
         ↓
  ml_fraud_predictions
         ↓
    ┌────┴────┐
    ↓         ↓
 Bank App   LangFlow
 (UI)       (AI Agent)
    ↓         ↓
 Fraud     Fraud
 Audit     Investigation
```

## LangFlow Agent Setup

1. Open LangFlow: http://localhost:7861
2. Create new flow
3. Add components:
   - Chat Input
   - Agent (connect to Ollama or OpenAI)
   - Postgres Query Tool (pgd, demo, postgres, secret)
   - Fraud Rules Search
   - Chat Output
4. Connect components
5. Test queries:
   - "Show me the top 10 fraud predictions from the last hour"
   - "What rules apply to US Stripe transactions over $5,000?"
   - "Explain why transaction 12345 was flagged"

## System Prompt

See [prompts/fraud-audit-system-prompt.md](prompts/fraud-audit-system-prompt.md) for the recommended LangFlow agent system prompt.
