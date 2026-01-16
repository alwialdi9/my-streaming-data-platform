# Streaming Data Platform (Confluent-like)

## Overview

This project is a **self-hosted streaming data platform** inspired by Confluent.  
It is designed to run both **locally** and in **production environments** (VM or Kubernetes).

The platform provides:
- **Connectors** (source & sink) for data ingestion and delivery
- **Apache Kafka** as the central streaming backbone
- **Apache Flink** for real-time stream processing and SQL
- A **Query Layer** for interactive stream queries
- A **Control Plane** (backend + frontend) to manage the entire system

The system follows a **Kafka-first architecture** with a strict separation between **data plane** and **control plane**.

---

## High-Level Architecture

```
                ┌────────────────────┐
                │     Frontend UI    │
                └─────────┬──────────┘
                          │
                ┌─────────▼──────────┐
                │    Control Plane   │
                │  (API + Metadata)  │
                └─────────┬──────────┘
                          │
        ┌─────────────────┼─────────────────┐
        │                 │                 │
┌───────▼───────┐ ┌───────▼────────┐ ┌───────▼────────┐
│   Connectors  │ │  Apache Kafka  │ │  Apache Flink  │
│ (Source/Sink) │ │ (Event Log)    │ │ (Stream SQL)   │
└───────┬───────┘ └───────┬────────┘ └───────┬────────┘
        │                 │                  │
┌───────▼───────┐ ┌───────▼────────┐ ┌───────▼────────┐
│   Databases   │ │  Processed     │ │   Query        │
│ (PostgreSQL)  │ │  Topics        │ │   Results      │
└───────────────┘ └────────────────┘ └────────────────┘
```

---

## Core Concepts

### 1. Kafka as the Data Contract

Apache Kafka is the **central contract** between all components.

- Connectors **produce to and consume from Kafka topics**
- Apache Flink **reads from Kafka and writes back to Kafka**
- Components never communicate directly with each other

This design provides:
- Loose coupling
- Replayability
- Fault tolerance
- Horizontal scalability

---

### 2. Changelog-Based Data Model (CDC)

The platform uses a **changelog stream model** for database changes.
Each change is represented as a structured event:
```json
{
  "op": "c | u | d | r",
  "before": { ... },
  "after": { ... },
  "ts_ms": 1700000000
}
```

Where:
- `c` = insert (create)
- `u` = update
- `d` = delete
- `r` = snapshot (initial read)

This model allows downstream systems to **reconstruct table state** correctly.

---

### 3. Connector Runtime
Connectors run inside a **connector runtime**, conceptually similar to Kafka Connect.
There are two connector types:
- **Source Connector**
  - Reads data from external systems
  - Publishes changelog events to Kafka
  - Example: PostgreSQL CDC

- **Sink Connector**
  - Consumes events from Kafka
  - Delivers data to external systems
  - Example: HTTP webhook, database, file system

The runtime is responsible for:
- Connector lifecycle management
- Offset tracking
- Error handling and retries
- Backpressure control

---

### 4. Stream Processing with Apache Flink

Apache Flink is used for:
- Real-time transformations
- Filtering
- Aggregations
- Joins
- Stateful stream processing

Flink treats Kafka changelog topics as **dynamic tables**.

Typical flow:
```
Raw CDC Topic → Flink SQL → Enriched Topic
```

Flink jobs are managed **outside of connectors** via the platform control plane.

---

### 5. Query Layer

The Query Layer provides a SQL-based interface for users to:
- Query streaming data
- Define transformations
- Materialize results into Kafka topics

Internally, this layer translates SQL statements into:
- Flink SQL jobs
- Managed Flink job lifecycles

---

### 6. Control Plane

The Control Plane acts as the **brain of the platform**.

Responsibilities:
- Connector CRUD operations
- Flink job submission and monitoring
- Topic and schema metadata
- Configuration validation
- Persistent state management

---

### 7. Frontend

The frontend provides a user interface to:
- Create and manage connectors
- Monitor connector and job status
- Submit streaming SQL queries
- Inspect logs and errors

---

## Project Structure

```
.
├── connectors/
├── runtime/
├── kafka/
├── flink/
├── query-engine/
├── control-plane/
├── frontend/
├── deploy/
└── docs/
```

---

## Deployment Modes

### Local Development
- Docker Compose
- Single-node Kafka
- Standalone Flink cluster

### Production
- Kafka cluster
- Flink on Kubernetes
- Scalable connector runtime
- Stateless control plane

---

## Design Principles

- Kafka-first architecture
- Clear separation between data plane and control plane
- Changelog-based streaming (CDC)
- Stateless connectors with externalized state
- Production-ready by design

---

## Roadmap

1. Data contract & topic conventions
2. Kafka setup
3. Connector runtime framework
4. Sink connectors
5. PostgreSQL CDC source
6. Apache Flink integration
7. Query layer
8. Control plane API
9. Frontend UI

---

## Status

Under active development.
