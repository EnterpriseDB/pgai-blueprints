# BFSI Fraud Detection Stack - Architecture

*For demonstration purposes only.*

## Overview

The BFSI Fraud Detection stack demonstrates a complete data platform for real-time fraud detection, combining transactional processing, analytics, machine learning, and AI-powered governance.

---

## 1. Deployment Topology

### Container Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           Docker Network: bfsi-network                           │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                        FOUNDATION PROFILE (default)                     │    │
│  ├─────────────────────────────────────────────────────────────────────────┤    │
│  │                                                                         │    │
│  │  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                  │    │
│  │  │     PGD     │    │   Kafka     │    │ ClickHouse  │                  │    │
│  │  │  (EDB PGD)  │    │  (KRaft)    │    │   (OLAP)    │                  │    │
│  │  │  :7434      │    │  :9096      │    │  :8125      │                  │    │
│  │  │             │    │             │    │             │                  │    │
│  │  │ • OLTP      │    │ • Events    │    │ • Analytics │                  │    │
│  │  │ • PGAA      │    │ • CDC       │    │ • MVs       │                  │    │
│  │  │ • AIDB      │    └─────────────┘    └─────────────┘                  │    │
│  │  └─────────────┘           │                  │                         │    │
│  │         │                  │                  │                         │    │
│  │         │           ┌──────┴──────┐           │                         │    │
│  │         │           │             │           │                         │    │
│  │  ┌──────┴─────┐  ┌──┴──────────┐  │   ┌───────┴─────┐                   │    │
│  │  │ RisingWave │  │   Kafka     │  │   │   MinIO     │                   │    │
│  │  │ (Streaming)│  │   Connect   │  │   │    (S3)     │                   │    │
│  │  │  :4574     │  │  (Debezium) │  │   │  :9003/9004 │                   │    │
│  │  │            │  │   :8084     │  │   │             │                   │    │
│  │  │ • CDC      │  │             │  │   │ • Iceberg   │                   │    │
│  │  │ • MVs      │  │ • PG→Kafka  │  │   │ • Artifacts │                   │    │
│  │  └────────────┘  └─────────────┘  │   │ • Docs      │                   │    │
│  │                                   │   └─────────────┘                   │    │
│  │  ┌─────────────┐  ┌─────────────┐ │   ┌─────────────┐                   │    │
│  │  │ Lakekeeper  │  │   MLflow    │ │   │  Metabase   │                   │    │
│  │  │  (Iceberg)  │  │ (ML Ops)    │ │   │ (Dashboards)│                   │    │
│  │  │   :8181     │  │  :5001      │ │   │   :3002     │                   │    │
│  │  │             │  │             │ │   │             │                   │    │
│  │  │ • Catalog   │  │ • Tracking  │ │   │ • BI        │                   │    │
│  │  │ • REST API  │  │ • Registry  │ │   │ • Queries   │                   │    │
│  │  └─────────────┘  │ • Gateway   │ │   └─────────────┘                   │    │
│  │                   └─────────────┘ │                                     │    │
│  │  ┌─────────────┐  ┌─────────────┐ │                                     │    │
│  │  │  Jupyter    │  │  LangFlow   │ │                                     │    │
│  │  │ (Notebooks) │  │  (Agents)   │ │                                     │    │
│  │  │   :8889     │  │   :7861     │ │                                     │    │
│  │  │             │  │             │ │                                     │    │
│  │  │ • Training  │  │ • Visual    │ │                                     │    │
│  │  │ • Analysis  │  │ • MCP       │ │                                     │    │
│  │  └─────────────┘  └─────────────┘ │                                     │    │
│  └───────────────────────────────────┴─────────────────────────────────────┘    │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                         OLTP PROFILE                                    │    │
│  ├─────────────────────────────────────────────────────────────────────────┤    │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                      │    │
│  │  │  Bank App   │  │ AIDB Init   │  │ MinIO Fraud │                      │    │
│  │  │  (Node.js)  │  │  (Semantic) │  │   Init      │                      │    │
│  │  │   :3001     │  │             │  │             │                      │    │
│  │  │             │  │ • Embeddings│  │ • Rule Docs │                      │    │
│  │  │ • 8 Tabs    │  │ • PGFS      │  │ • Upload    │                      │    │
│  │  │ • Simulator │  └─────────────┘  └─────────────┘                      │    │
│  │  └─────────────┘                                                        │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                          ML PROFILE                                     │    │
│  ├─────────────────────────────────────────────────────────────────────────┤    │
│  │  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐       │    │
│  │  │ Feature          │  │  ML-Inference    │  │  ML-Inference    │       │    │
│  │  │ Materializer     │  │     Kafka        │  │   RisingWave     │       │    │
│  │  │                  │  │                  │  │                  │       │    │
│  │  │ Kafka→Enriched   │  │                  │  │                  │       │    │
│  │  └──────────────────┘  └──────────────────┘  └──────────────────┘       │    │
│  │  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐       │    │
│  │  │  ML-Inference    │  │  ML-Inference    │  │   Fraud Alert    │       │    │
│  │  │   ClickHouse     │  │      PGAA        │  │    Service       │       │    │
│  │  │                  │  │                  │  │                  │       │    │
│  │  └──────────────────┘  └──────────────────┘  └──────────────────┘       │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                       AIRFLOW PROFILE (opt-in)                          │    │
│  ├─────────────────────────────────────────────────────────────────────────┤    │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐     │    │
│  │  │   Airflow   │  │   Airflow   │  │   Airflow   │  │   Airflow   │     │    │
│  │  │  Webserver  │  │  Scheduler  │  │    Init     │  │  Postgres   │     │    │
│  │  │    :8888    │  │             │  │             │  │             │     │    │
│  │  │             │  │             │  │             │  │             │     │    │
│  │  │ • UI / Auth │  │ • DAG Exec  │  │ • One-shot  │  │ • Metadata  │     │    │
│  │  │ • Log View  │  │ • Triggers  │  │   bootstrap │  │   DB        │     │    │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘     │    │
│  │                                                                         │    │
│  │  DAG: fraud_reconciliation (every 2 min)                                │    │
│  │  PGD ⇄ ClickHouse FINAL ⇄ RisingWave DISTINCT → diff_and_alert          │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Service Summary

| Component          | Image                       | Port       | Purpose                                 |
|--------------------|-----------------------------|------------|-----------------------------------------|
| **PGD**            | EDB PGD + PGAA + AIDB       | 7434       | OLTP database with analytics extensions |
| **Kafka**          | apache/kafka                | 9096       | Event streaming (KRaft mode)            |
| **Kafka Connect**  | debezium/connect:2.4        | 8084       | CDC from PostgreSQL                     |
| **ClickHouse**     | clickhouse-server           | 8125       | OLAP analytics                          |
| **RisingWave**     | risingwavelabs/risingwave   | 4574       | Streaming SQL                           |
| **MinIO**          | minio/minio                 | 9003/9004  | S3 storage (Iceberg, MLflow)            |
| **Lakekeeper**     | lakekeeper/catalog          | 8181       | Iceberg REST catalog                    |
| **MLflow**         | mlflow                      | 5001       | ML tracking & model registry            |
| **Jupyter**        | datascience-notebook        | 8889       | ML development                          |
| **LangFlow**       | langflowai/langflow         | 7861       | Visual agent builder                    |
| **Metabase**       | metabase/metabase           | 3002       | BI dashboards                           |
| **Bank App**       | Node.js (custom)            | 3001       | Transaction simulator                   |
| **Airflow**        | apache/airflow:2.10.4       | 8888       | Workflow orchestration (opt-in profile) |

---

## 2. Data Flow Architecture

### End-to-End Data Pipeline

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              DATA FLOW ARCHITECTURE                             │
└─────────────────────────────────────────────────────────────────────────────────┘

                          ┌─────────────────┐
                          │    Bank App     │
                          │   (Simulator)   │
                          └────────┬────────┘
                                   │ INSERT transactions
                                   ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                                    PGD (OLTP)                                    │
│  ┌─────────────────────────────────────────────────────────────────────────────┐ │
│  │  transactions │ customers │ merchants │ fraud_rules │ fraud_predictions     │ │
│  └─────────────────────────────────────────────────────────────────────────────┘ │
│                     │                │                         ▲                 │
│                     │ WAL            │ WAL                     │ ML writes       │
└─────────────────────┼────────────────┼─────────────────────────┼─────────────────┘
                      │                │                         │
        ┌─────────────┼────────────────┼─────────────────────────┘
        │             │                │
        │             │                │
        ▼             ▼                ▼
┌───────────────┐ ┌────────────┐ ┌─────────────┐
│  RisingWave   │ │   Kafka    │ │    PGAA     │
│  (Direct CDC) │ │ (Debezium) │ │ (WAL Sync)  │
└───────┬───────┘ └──────┬─────┘ └──────┬──────┘
        │                │              │
        │ Streaming      │ Topics       │ Iceberg
        │ MVs            │              │ Tables
        ▼                ▼              ▼
┌───────────────┐ ┌─────────────────┐ ┌─────────────┐
│  mv_tx_*      │ │ corebanking.    │ │ Lakekeeper  │
│  mv_fraud_*   │ │ public.         │ │  + MinIO    │
│               │ │ transactions    │ │             │
└───────┬───────┘ └───────┬─────────┘ └─────┬───────┘
        │                 │                 │
        │         ┌───────┴────────┐        │
        │         │                │        │
        │         ▼                ▼        │
        │  ┌────────────┐   ┌───────────┐   │
        │  │ ClickHouse │   │ Feature   │   │
        │  │  (Tables)  │   │Materializer   │
        │  └─────┬──────┘   └─────┬─────┘   │
        │        │                │         │
        │        ▼                ▼         │
        │  ┌────────────┐   ┌───────────┐   │
        │  │ ClickHouse │   │ Enriched  │   │
        │  │    MVs     │   │  Topic    │   │
        │  └─────┬──────┘   └─────┬─────┘   │
        │        │                │         │
        ▼        ▼                ▼         ▼
┌───────────────────────────────────────────────────────────────┐
│                    ML INFERENCE ENGINES                       │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌──────────┐ │
│  │ ML-RisingW  │ │ ML-ClickH   │ │  ML-Kafka   │ │ ML-PGAA  │ │
│  └──────┬──────┘ └──────┬──────┘ └──────┬──────┘ └────┬─────┘ │
└─────────┼───────────────┼───────────────┼─────────────┼───────┘
          │               │               │             │
          └───────────────┴───────────────┴─────────────┘
                                   │
                                   ▼
                    ┌──────────────────────────┐
                    │   fraud_predictions      │
                    │   (PGD - OLTP)           │
                    └────────────┬─────────────┘
                                 │
              ┌──────────────────┼──────────────────┐
              │                  │                  │
              ▼                  ▼                  ▼
     ┌────────────────┐ ┌──────────────┐ ┌─────────────────┐
     │  Fraud Alert   │ │   Metabase   │ │    LangFlow     │
     │   Service      │ │  Dashboards  │ │  Audit Agents   │
     │                │ │              │ │                 │
     │  • Webhooks    │ │  • Metrics   │ │  • RAG          │
     │  • Thresholds  │ │  • Trends    │ │  • Investigation│
     └────────────────┘ └──────────────┘ └─────────────────┘
```

### Auxiliary Flows (run alongside the main pipeline)

**Hybrid Search (UC2 extension)**

```
                  ┌──────────────────────────┐
                  │           PGD            │
                  └─────────────┬────────────┘
                                ▼
                  ┌──────────────────────────┐
                  │ transactions_hybrid_     │
                  │        corpus            │
                  └──────┬─────────────┬─────┘
                         │             │
                         ▼             ▼
              ┌──────────────────┐  ┌──────────────────┐
              │  VectorChord     │  │   AIDB BERT      │
              │     BM25         │  │  encode_text     │
              │   (lexical)      │  │   (semantic)     │
              └────────┬─────────┘  └────────┬─────────┘
                       │                     │
                       └──────────┬──────────┘
                                  ▼
                        ┌──────────────────┐
                        │   RRF fusion     │
                        └────────┬─────────┘
                                 ▼
                        ┌──────────────────┐
                        │     Metabase     │
                        │ comparison cards │
                        └──────────────────┘
```

**Drift Reconciliation (UC2 extension, opt-in Airflow profile)**

```
   ┌────────────┐   ┌────────────────┐   ┌───────────────────┐
   │    PGD     │   │  CH (FINAL)    │   │  RW (DISTINCT)    │
   └─────┬──────┘   └────────┬───────┘   └─────────┬─────────┘
         │                   │                     │
         └───────────────────┼─────────────────────┘
                             ▼
              ┌──────────────────────────────────┐
              │  Airflow DAG:                    │
              │  fraud_reconciliation (2 min)    │
              └─────────────────┬────────────────┘
                                ▼
                      ┌──────────────────┐
                      │  diff_and_alert  │
                      └────────┬─────────┘
                               ▼
                      ┌────────────────────────┐
                      │      Drift Alert       │
                      │ (log + Workspace UI)   │
                      └────────────────────────┘
```

Both flows are illustrated in full under §3 UC2 Extensions below.

### CDC Paths Comparison

| Path           | Source   | Transport                      | Latency | Use Case          |
|----------------|----------|--------------------------------|---------|-------------------|
| **Kafka**      | PGD WAL  | Debezium → Kafka → ClickHouse  | ~3-5s   | Batch analytics   |
| **RisingWave** | PGD WAL  | Native CDC                     | ~1-2s   | Streaming MVs     |
| **PGAA**       | PGD WAL  | Internal WAL sync              | ~1s     | Iceberg analytics |

---

## 3. Use Case Architectures

### UC1: Transactional Workloads (OLTP)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    USECASE 1: TRANSACTIONAL WORKLOADS                       │
└─────────────────────────────────────────────────────────────────────────────┘

    ┌─────────────────┐
    │   Bank App UI   │────────────────────────────────────┐
    │                 │                                    │
    │ • Transactions  │                                    │
    │ • Accounts      │                                    │
    │ • Transfer      │                                    │
    └────────┬────────┘                                    │
             │ HTTP API                                    │
             ▼                                             │
    ┌─────────────────┐                                    │
    │   Bank App      │                                    │
    │   (Node.js)     │                                    │
    │    :3001        │                                    │
    └────────┬────────┘                                    │
             │                                             │
             │ SQL (INSERT/UPDATE/SELECT)                  │
             ▼                                             │
    ┌──────────────────────────────────────────────┐       │
    │                  PGD (EDB)                   │       │
    │  ┌──────────────────────────────────────┐    │       │
    │  │          OLTP Schema (demo)          │    │       │
    │  ├──────────────────────────────────────┤    │       │
    │  │ • customers       (100K records)     │    │       │
    │  │ • merchants       (1K records)       │    │       │
    │  │ • transactions    (1M+ records)      │    │       │
    │  │ • fraud_rules     (15 rules)         │    │       │
    │  │ • fraud_labels    (trigger-based)    │    │       │
    │  └──────────────────────────────────────┘    │       │
    │                                              │       │
    │  Features:                                   │       │
    │  • ACID transactions                         │◄──────┘
    │  • Foreign key constraints                   │  Real-time
    │  • Trigger-based fraud labeling              │  Updates
    │  • Connection pooling                        │
    └──────────────────────────────────────────────┘

    Transaction Simulator:
    ┌─────────────────────────────────────────────┐
    │  POST /api/sim/start                        │
    │  {                                          │
    │    "txTypes": ["CREDIT","DEBIT","TRANSFER"],│
    │    "fraudMode": "occasional",               │
    │    "intervalMs": 1000                       │
    │  }                                          │
    │                                             │
    │  → Generates ~1 tx/second with fraud        │
    │    patterns matching rule definitions       │
    └─────────────────────────────────────────────┘
```

---

### UC2: Operational Dashboards (Real-Time Metrics)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                  USECASE 2: OPERATIONAL DASHBOARDS (OLAP)                   │
└─────────────────────────────────────────────────────────────────────────────┘

                        ┌──────────────────┐
                        │     Metabase     │
                        │    Dashboards    │
                        │      :3002       │
                        └────────┬─────────┘
                                 │
              ┌──────────────────┼──────────────────┐
              │                  │                  │
              ▼                  ▼                  ▼
    ┌─────────────────┐ ┌───────────────┐ ┌─────────────────┐
    │   ClickHouse    │ │  RisingWave   │ │       PGD       │
    │     :8125       │ │    :4574      │ │     :7434       │
    │                 │ │               │ │                 │
    │ Batch Analytics │ │ Streaming MVs │ │   OLTP Data     │
    └────────┬────────┘ └───────┬───────┘ └────────┬────────┘
             │                  │                  │
             │                  │                  │
    ┌────────┴──────────────────┴──────────────────┴────────┐
    │                    DATA SOURCES                       │
    └────────┬──────────────────┬──────────────────┬────────┘
             │                  │                  │
             ▼                  ▼                  ▼
    ┌─────────────────┐ ┌───────────────┐ ┌─────────────────┐
    │   Debezium      │ │  RW Native    │ │   Direct WAL    │
    │  Kafka Connect  │ │     CDC       │ │     Sync        │
    └────────┬────────┘ └───────┬───────┘ └────────┬────────┘
             │                  │                  │
             └──────────────────┴──────────────────┘
                                │
                                ▼
                       ┌────────────────┐
                       │      PGD       │
                       │   (Source)     │
                       └────────────────┘

    ┌─────────────────────────────────────────────────────────────────────────┐
    │                        MATERIALIZED VIEWS                               │
    ├─────────────────────────────────────────────────────────────────────────┤
    │                                                                         │
    │  ClickHouse MVs (Batch - 1min refresh):                                 │
    │  ┌────────────────────────────────────────────────────────────────────┐ │
    │  │ • mv_tx_by_type         - Transaction counts by type               │ │
    │  │ • mv_tx_by_merchant     - Volume by merchant                       │ │
    │  │ • mv_fraud_by_hour      - Fraud patterns over time                 │ │
    │  │ • mv_customer_risk      - Customer risk scores                     │ │
    │  └────────────────────────────────────────────────────────────────────┘ │
    │                                                                         │
    │  RisingWave MVs (Streaming - sub-second):                               │
    │  ┌────────────────────────────────────────────────────────────────────┐ │
    │  │ • mv_realtime_tx_stats  - Live transaction metrics                 │ │
    │  │ • mv_fraud_velocity     - Real-time fraud rate                     │ │
    │  │ • mv_merchant_24h       - Rolling 24h merchant stats               │ │
    │  └────────────────────────────────────────────────────────────────────┘ │
    │                                                                         │
    └─────────────────────────────────────────────────────────────────────────┘

    Dashboard Metrics:
    ┌────────────────────────────────────────────────────────┐
    │ • Transaction Volume (real-time)                       │
    │ • Fraud Detection Rate                                 │
    │ • ML Model Performance (Precision/Recall)              │
    │ • Time-to-Detection-of-Fraud (TTDF) by path            │
    │ • Regional fraud patterns                              │
    │ • Merchant risk rankings                               │
    └────────────────────────────────────────────────────────┘
```

See **UC2 Extensions** below for the hybrid-search Metabase cards (BM25 + AIDB BERT + RRF) and the opt-in Airflow `fraud_reconciliation` DAG that ships with this use case.

---

### UC2 Extensions: Hybrid Search and Drift Reconciliation

These two capabilities ship inside UC2 and surface as additional Metabase cards plus an opt-in Airflow workflow.

**A. Hybrid Search — Lexical + Semantic with Reciprocal Rank Fusion**

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            HYBRID SEARCH (UC2)                              │
└─────────────────────────────────────────────────────────────────────────────┘

         User Query: "fraudulent ATM withdrawal late at night"
                                  │
                                  ▼
    ┌──────────────────────────────────────────────────────────────────┐
    │   transactions_hybrid_corpus  (materialized from OLTP rows)      │
    └──────────────────────────────────────────────────────────────────┘
              │                                          │
       Lexical arm                                Semantic arm
              │                                          │
              ▼                                          ▼
    ┌──────────────────┐                       ┌──────────────────┐
    │  VectorChord-    │                       │      AIDB        │
    │  BM25            │                       │  BERT Embedding  │
    │  + pg-tokenizer  │                       │  via encode_text │
    │  (PGD extension) │                       │  (per-row insert)│
    │                  │                       │                  │
    │  Top-N by BM25   │                       │  Top-N by cosine │
    └────────┬─────────┘                       └────────┬─────────┘
             │                                          │
             └──────────────────┬───────────────────────┘
                                │
                                ▼
                  ┌─────────────────────────┐
                  │ Reciprocal Rank Fusion  │
                  │ (in-database SQL)       │
                  │                         │
                  │ • Merged ranking        │
                  │ • found_by attribution  │
                  └────────────┬────────────┘
                               │
                               ▼
                  ┌─────────────────────────┐
                  │  Metabase Comparison    │
                  │  cards: BM25 / BERT /   │
                  │  Hybrid (RRF) / Rerank  │
                  └─────────────────────────┘
```

**B. Airflow Drift Reconciliation — `fraud_reconciliation` DAG**

```
┌─────────────────────────────────────────────────────────────────────────────┐
│            AIRFLOW DAG: fraud_reconciliation  (every 2 min)                 │
└─────────────────────────────────────────────────────────────────────────────┘

   ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
   │   count_pgd     │   │ count_clickhouse│   │ count_risingwave│
   │   (source)      │   │  (FINAL view)   │   │ (DISTINCT view) │
   └────────┬────────┘   └────────┬────────┘   └────────┬────────┘
            │                     │                     │
            └──────────┬──────────┴─────────────────────┘
                       │
                       ▼
              ┌────────────────────┐
              │   diff_and_alert   │
              │                    │
              │ • Compute % drift  │
              │ • Compare to       │
              │   DRIFT_THRESHOLD  │
              └─────────┬──────────┘
                        │  (if exceeded)
                        ▼
                ┌────────────────┐
                │  Drift Alert   │
                │  Airflow log + │
                │  Workspace UI  │
                └────────────────┘

   Notes:
   • RisingWave count uses count(DISTINCT tx_id) to dedup CDC INSERT+UPDATE events.
   • ClickHouse count uses FINAL over the ReplacingMergeTree.
   • Drift threshold is set per deployment via DRIFT_THRESHOLD_PCT.
```

---

### UC3: Machine Learning and Inferencing

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                   USECASE 3: ML FRAUD DETECTION                             │
└─────────────────────────────────────────────────────────────────────────────┘

                    MODEL TRAINING PIPELINE
    ┌────────────────────────────────────────────────────────────────────────┐
    │                                                                        │
    │  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                 │
    │  │   Jupyter   │───▶│   MLflow    │───▶│   MinIO     │                 │
    │  │  Notebook   │    │  Tracking   │    │  Artifacts  │                 │
    │  │             │    │             │    │             │                 │
    │  │ XGBoost     │    │ Experiments │    │ Models PKL  │                 │
    │  │ Training    │    │ Metrics     │    │ Params      │                 │
    │  └─────────────┘    └──────┬──────┘    └─────────────┘                 │
    │                            │                                           │
    │                            ▼                                           │
    │                    ┌─────────────┐                                     │
    │                    │   Model     │                                     │
    │                    │  Registry   │                                     │
    │                    │             │                                     │
    │                    │ Production  │                                     │
    │                    │  Staging    │                                     │
    │                    └──────┬──────┘                                     │
    │                           │                                            │
    └───────────────────────────┼────────────────────────────────────────────┘
                                │
                                ▼
                    INFERENCE PIPELINES (4 Paths)
    ┌────────────────────────────────────────────────────────────────────────┐
    │                                                                        │
    │  PATH 1: Kafka (Fastest - <100ms TTDF)                                 │
    │  ┌──────────────────────────────────────────────────────────────────┐  │
    │  │  PGD ──WAL──▶ Debezium ──▶ Kafka ──▶ Feature    ──▶ ML-Kafka     │  │
    │  │                           topic     Materializer     XGBoost     │  │
    │  │                                         │              │         │  │
    │  │                                    Enriched       Predictions    │  │
    │  │                                     Topic              │         │  │
    │  │                                                        ▼         │  │
    │  │                                                fraud_predictions │  │
    │  └──────────────────────────────────────────────────────────────────┘  │
    │                                                                        │
    │  PATH 2: PGAA/Iceberg (~300ms TTDF)                                    │
    │  ┌──────────────────────────────────────────────────────────────────┐  │
    │  │  PGD ──WAL──▶ PGAA ──▶ Iceberg ──▶ CTAS Feature ──▶ ML-PGAA      │  │
    │  │              Sync       Tables       Tables (30s)    XGBoost     │  │
    │  │                                                         │        │  │
    │  │                                                    Predictions   │  │
    │  │                                                         │        │  │
    │  │                                                         ▼        │  │
    │  │                                                fraud_predictions │  │
    │  └──────────────────────────────────────────────────────────────────┘  │
    │                                                                        │
    │  PATH 3: ClickHouse (~3000ms TTDF)                                     │
    │  ┌──────────────────────────────────────────────────────────────────┐  │
    │  │  PGD ──WAL──▶ Debezium ──▶ Kafka ──▶ ClickHouse ──▶ ML-CH        │  │
    │  │                                       MVs           XGBoost      │  │
    │  │                                        │               │         │  │
    │  │                                   Feature Query   Predictions    │  │
    │  │                                                        │         │  │
    │  │                                                        ▼         │  │
    │  │                                                fraud_predictions │  │
    │  └──────────────────────────────────────────────────────────────────┘  │
    │                                                                        │
    │  PATH 4: RisingWave (~4200ms TTDF)                                     │
    │  ┌──────────────────────────────────────────────────────────────────┐  │
    │  │  PGD ──CDC──▶ RisingWave ──▶ Streaming ──▶ ML-RW                 │  │
    │  │              Native          MVs           XGBoost               │  │
    │  │                               │               │                  │  │
    │  │                          Feature Poll    Predictions             │  │
    │  │                                               │                  │  │
    │  │                                               ▼                  │  │
    │  │                                       fraud_predictions          │  │
    │  └──────────────────────────────────────────────────────────────────┘  │
    │                                                                        │
    └────────────────────────────────────────────────────────────────────────┘

    ML Features (XGBoost Model):
    ┌────────────────────────────────────────────────────────────────────────┐
    │ • amount                    - Transaction amount                       │
    │ • tx_count_1h               - Transactions in last hour                │
    │ • tx_sum_1h                 - Total amount in last hour                │
    │ • avg_tx_amount             - Customer average transaction             │
    │ • merchant_fraud_rate       - Historical merchant fraud rate           │
    │ • time_since_last_tx        - Seconds since previous transaction       │
    │ • is_weekend                - Weekend indicator                        │
    │ • hour_of_day               - Transaction hour                         │
    └────────────────────────────────────────────────────────────────────────┘

    Performance Comparison:
    ┌─────────────┬─────────────┬─────────────┬─────────────────────────────┐
    │    Path     │    TTDF     │   Latency   │        Best For             │
    ├─────────────┼─────────────┼─────────────┼─────────────────────────────┤
    │ Kafka       │   <100ms    │   Real-time │ Blocking fraud prevention   │
    │ PGAA        │   ~300ms    │   Near-RT   │ Integrated analytics        │
    │ ClickHouse  │   ~3000ms   │   Batch     │ Historical analysis         │
    │ RisingWave  │   ~4200ms   │   Streaming │ Complex aggregations        │
    └─────────────┴─────────────┴─────────────┴─────────────────────────────┘
```

---

### UC4 / UC5 / UC6: ML Governance, GenAI Fraud Audit, AI Governance

The diagrams below cover three of the README's use cases together because they share components (MLflow tracking, LangFlow agent, LLM judge). For canonical scope per use case see the README:

- **UC4 — ML Governance**: MLflow experiment tracking and model registry (the "ML Governance (UC4)" block in the diagram below).
- **UC5 — GenAI Fraud Audit**: the LangFlow agent under "AI AGENT ARCHITECTURE".
- **UC6 — AI Governance**: LLM tracing and the LLM-judge evaluation pipeline (the "AI Governance (UC6)" block and the Evaluation Pipeline).

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                  USECASES 4-6: ML GOV, GENAI AUDIT, AI GOV                  │
└─────────────────────────────────────────────────────────────────────────────┘

                         AI AGENT ARCHITECTURE
    ┌─────────────────────────────────────────────────────────────────────────┐
    │                                                                         │
    │                      ┌─────────────────┐                                │
    │                      │   User Query    │                                │
    │                      │                 │                                │
    │                      │ "Investigate    │                                │
    │                      │  TX-12345"      │                                │
    │                      └────────┬────────┘                                │
    │                               │                                         │
    │                               ▼                                         │
    │  ┌──────────────────────────────────────────────────────────────────┐   │
    │  │                       LangFlow Agent                             │   │
    │  │                         :7861                                    │   │
    │  │  ┌──────────────────────────────────────────────────────────┐    │   │
    │  │  │              ML Algorithm Audit Agent                    │    │   │
    │  │  │                                                          │    │   │
    │  │  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌─────────┐   │    │   │
    │  │  │  │  Router  │─▶│   RAG    │─▶│  Query   │─▶│  LLM    │   │    │   │
    │  │  │  │  (HITL)  │  │ (Rules)  │  │(Postgres)│  │(Claude) │   │    │   │
    │  │  │  └──────────┘  └──────────┘  └──────────┘  └─────────┘   │    │   │
    │  │  │       │              │             │             │       │    │   │
    │  │  └───────┼──────────────┼─────────────┼─────────────┼───────┘    │   │
    │  │          │              │             │             │            │   │
    │  └──────────┼──────────────┼─────────────┼─────────────┼────────────┘   │
    │             │              │             │             │                │
    │             ▼              ▼             ▼             ▼                │
    │      ┌───────────┐  ┌───────────┐  ┌───────────┐  ┌───────────┐         │
    │      │  Human    │  │   MinIO   │  │ PG-Airman │  │  Bedrock  │         │
    │      │ Approval  │  │   AIDB    │  │    MCP    │  │  Claude   │         │
    │      │           │  │           │  │           │  │           │         │
    │      │ High-risk │  │ • Fraud   │  │ • SQL     │  │ • Sonnet  │         │
    │      │ Actions   │  │   Rules   │  │   Query   │  │ • Haiku   │         │
    │      │           │  │ • Vectors │  │ • Schema  │  │           │         │
    │      └───────────┘  └───────────┘  └───────────┘  └───────────┘         │
    │                                                                         │
    └─────────────────────────────────────────────────────────────────────────┘

                         GOVERNANCE ARCHITECTURE
    ┌─────────────────────────────────────────────────────────────────────────┐
    │                                                                         │
    │  ┌──────────────────────────────────────────────────────────────────┐   │
    │  │                      MLflow Tracking                             │   │
    │  │                         :5001                                    │   │
    │  │                                                                  │   │
    │  │  ┌───────────────────────────────────────────────────────────┐   │   │
    │  │  │                  ML Governance (UC4)                      │   │   │
    │  │  ├───────────────────────────────────────────────────────────┤   │   │
    │  │  │ • Experiment Tracking   - All training runs               │   │   │
    │  │  │ • Model Registry        - Version control                 │   │   │
    │  │  │ • Model Lineage         - Data→Model→Prediction           │   │   │
    │  │  │ • Metrics History       - Precision, Recall, F1           │   │   │
    │  │  │ • Artifact Storage      - Models, configs (MinIO)         │   │   │
    │  │  └───────────────────────────────────────────────────────────┘   │   │
    │  │                                                                  │   │
    │  │  ┌───────────────────────────────────────────────────────────┐   │   │
    │  │  │                  AI Governance (UC6)                      │   │   │
    │  │  ├───────────────────────────────────────────────────────────┤   │   │
    │  │  │ • LLM Tracing          - All agent interactions           │   │   │
    │  │  │   (via Traceloop/OTLP)                                    │   │   │
    │  │  │ • Token Usage          - Cost tracking                    │   │   │
    │  │  │ • Latency Metrics      - Response times                   │   │   │
    │  │  │ • Evaluation Runs      - LLM Judge assessments            │   │   │
    │  │  │ • Prompt History       - Input/output logging             │   │   │
    │  │  └───────────────────────────────────────────────────────────┘   │   │
    │  │                                                                  │   │
    │  └──────────────────────────────────────────────────────────────────┘   │
    │                                                                         │
    │  ┌──────────────────────────────────────────────────────────────────┐   │
    │  │                     Evaluation Pipeline                          │   │
    │  │                                                                  │   │
    │  │  ┌───────────┐    ┌───────────┐    ┌───────────┐                 │   │
    │  │  │  Jupyter  │───▶│  Agent    │───▶│   LLM     │                 │   │
    │  │  │ Notebook  │    │ Harness   │    │  Judge    │                 │   │
    │  │  │           │    │           │    │ (Claude)  │                 │   │
    │  │  │ Test      │    │ Execute   │    │           │                 │   │
    │  │  │ Cases     │    │ + Trace   │    │ Score     │                 │   │
    │  │  └───────────┘    └───────────┘    └─────┬─────┘                 │   │
    │  │                                          │                       │   │
    │  │                                          ▼                       │   │
    │  │                                   ┌───────────┐                  │   │
    │  │                                   │  MLflow   │                  │   │
    │  │                                   │  Metrics  │                  │   │
    │  │                                   └───────────┘                  │   │
    │  │                                                                  │   │
    │  └──────────────────────────────────────────────────────────────────┘   │
    │                                                                         │
    └─────────────────────────────────────────────────────────────────────────┘

    Agent Tools:
    ┌────────────────────────────────────────────────────────────────────────┐
    │ • PG-Airman MCP      - Database queries via MCP protocol               │
    │ • AIDB RAG           - Semantic search over fraud rules (BERT)         │
    │ • ML Audit Tool      - Query model predictions and metrics             │
    │ • HITL Router        - Human-in-the-loop for high-risk actions         │
    └────────────────────────────────────────────────────────────────────────┘

    Evaluation Metrics:
    ┌────────────────────────────────────────────────────────────────────────┐
    │ • Correctness        - Factual accuracy of responses                   │
    │ • Relevance          - Answer addresses the question                   │
    │ • Groundedness       - Claims supported by retrieved context           │
    │ • Harmlessness       - No harmful content generated                    │
    │ • Tool Usage         - Appropriate tool selection                      │
    └────────────────────────────────────────────────────────────────────────┘
```

---

## Summary

| Use Case             | Components                     | Key Metrics                            |
|----------------------|--------------------------------|----------------------------------------|
| **UC1: OLTP**        | Bank App → PGD                 | Transaction TPS, Latency               |
| **UC2: OLAP**        | PGD → Kafka/RW/CH → Metabase   | Query latency, Dashboard refresh       |
| **UC2 Ext: Hybrid**  | AIDB BERT + VectorChord-BM25 + RRF | Recall, found_by attribution       |
| **UC2 Ext: Drift**   | Airflow `fraud_reconciliation` DAG | Drift % vs DRIFT_THRESHOLD_PCT     |
| **UC3: ML Fraud**    | 4 inference paths → XGBoost    | TTDF (<100ms to ~4s), Precision/Recall |
| **UC4: ML Gov**      | MLflow Tracking + Registry     | Model versions, Experiment history     |
| **UC5: GenAI Audit** | LangFlow → LLM + Tools         | Correctness, Groundedness, Latency     |
| **UC6: AI Gov**      | MLflow Traces + LLM Judge      | Token usage, Evaluation scores         |
