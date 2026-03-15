# Qute
## Simple Distributed Message Queue

Qute is a distributed message queue built in Go. It implements a commit log at its core, extended with **topics** and **partitions** for organized, parallel message streaming across a cluster.

> ⚠️ **This is a learning project.** It is not production-ready software. Built to understand distributed systems fundamentals — not as a replacement for Kafka or RabbitMQ.

---

## Features

- **Distributed commit log** — append-only, ordered record storage across nodes
- **Topics** — named channels that producers publish messages to and consumers read from
- **Partitions** — each topic is split into partitions for parallel reads and writes
- **Cluster membership & replication** — nodes discover each other and replicate logs for fault tolerance
- **gRPC API** — fast, strongly-typed binary communication between clients and nodes

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Go |
| API | gRPC |
| Serialization | Protocol Buffers (Protobuf) |
| Consensus | Raft |

---

## Architecture

```
Producer
   │
   ▼
[ Topic: "orders" ]
   ├── Partition 0  →  [ Node A (Leader) ]  →  [ Node B (Follower) ]
   ├── Partition 1  →  [ Node C (Leader) ]  →  [ Node A (Follower) ]
   └── Partition 2  →  [ Node B (Leader) ]  →  [ Node C (Follower) ]
                                                        │
                                                   Consumer
```

- Producers publish to a **topic**. Messages are routed to a partition (by key or round-robin).
- Each partition is a replicated commit log managed by a **Raft** consensus group.
- Consumers subscribe to a topic and read from one or more partitions.

---

## Getting Started

### Prerequisites

- Go 1.21+
- `protoc` compiler with the Go and gRPC plugins

```bash
# Install protoc plugins
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

### Build

```bash
git clone https://github.com/Afrawles/qute.git
cd qute
go build ./...
```

### Run a Single Node

```bash
```

### Run a 3-Node Cluster

```bash

```

---

## Usage

### Produce a Message

```go

```

### Consume Messages

```go

```

---

## Project Structure

```
qute/
```

---

## Roadmap

- [ ] Consumer groups with offset tracking
- [ ] Topic auto-creation via API
- [ ] Partition rebalancing
- [ ] CLI client (`qute produce / qute consume`)
- [ ] Metrics endpoint (Prometheus)

---

## Status

**Work in progress.** Core log, gRPC server, and Raft replication are functional. Topics and partitions are actively being built on top of the base.

---

<sub>Core log architecture based on guidance from *Distributed Services with Go* by Travis Jeffery (Pragmatic Bookshelf). Topics and partitions are original extensions.</sub>
