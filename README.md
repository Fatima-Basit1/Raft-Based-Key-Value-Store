# Raft-Based Key-Value Store

This project is a basic distributed key-value store built using the Raft consensus algorithm. It supports basic operations like `put`, `append`, and `get`, while maintaining consistency and handling leader election, replication, and failover.

## Features

- Leader Election using randomized timeouts
- Log Replication across nodes
- State Machine Replication with `put`, `append`, and `get` operations
- Fault Tolerance: Handles leader failure, log consistency, and network partitions
- Exposes REST API endpoints + supports CLI interaction
- Thread-safe using Go mutexes & goroutines

## Quick Start

### 1. Run 3 Nodes Locally

```bash
go run Code_A02.go -id 0 -port 8000 -peers "localhost:8001,localhost:8002"
go run Code_A02.go -id 1 -port 8001 -peers "localhost:8000,localhost:8002"
go run Code_A02.go -id 2 -port 8002 -peers "localhost:8000,localhost:8001"
```

Each node will launch its own HTTP server and CLI.

### 2. Use the CLI

Once running, each node provides an interactive terminal. You can:

```
put <key> <value>     # Store a key-value pair
append <key> <value>  # Append to an existing value
get <key>             # Fetch a value
exit                  # Exit CLI
```

Only the leader can handle write operations (`put`, `append`). Others will reject them with a "Not Leader" message.

## Internals 

- The system follows Raft's structure with Follower, Candidate, and Leader states.
- Each node maintains logs, current term, votedFor, commit index, etc.
- On leader failure, a new election is triggered automatically.
- Log entries are committed when a majority of nodes replicate them.
- A simple in-memory key-value map stores the state machine's data.




