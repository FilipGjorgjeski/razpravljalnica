# razpravljalnica CLI guide

This document shows how to run every implemented RPC via the `razcli` command-line tool. The CLI talks to the control plane and the data-plane nodes using gRPC.

## Prerequisites
- Go installed and `$GOBIN` on PATH.
- From the repo root, build the CLI (or run with `go run`):
  - Build: `go build -o bin/razcli ./cmd/razcli`
  - Run in place: `go run ./cmd/razcli --help`
- Control plane and nodes must be running and reachable. Default control-plane address: `127.0.0.1:50050`.

Start control plane and nodes (example):
```
# control plane (choose seed IDs for the chain)
go run ./cmd/razpravljalnica-control-plane --chain n1

# node n1 (head/tail when alone)
go run ./cmd/razpravljalnica-node \
  --node-id n1 \
  --listen 127.0.0.1:6001 \
  --advertise 127.0.0.1:6001 \
  --control-plane 127.0.0.1:50050

# add and start another node (n2)
bin/razcli node add --id n2 --addr 127.0.0.1:6002
go run ./cmd/razpravljalnica-node \
  --node-id n2 \
  --listen 127.0.0.1:6002 \
  --advertise 127.0.0.1:6002 \
  --control-plane 127.0.0.1:50050
```

Global flags (apply to all razcli commands):
- `--control-plane <addr>`: control plane gRPC address (default `127.0.0.1:50050`).
- `--timeout <duration>`: per-RPC timeout (default `5s`).
- `--addr <addr>`: force data-plane RPCs to a specific node (reads/writes/subs) instead of letting control-plane assign.
- `--no-redirect`: when set, writes fail instead of redirecting if they hit a non-head node (useful for testing failures).
- `--watch-for <duration>`: used with `cluster --watch-for` to stream cluster changes for the duration.

Control-plane flags (`razpravljalnica-control-plane`):
- `--chain <id[,id2,...]>`: initial chain node IDs (comma-separated). At least one ID required.
- `--listen <addr>` (if present): address the control plane listens on (default `127.0.0.1:50050`).

Node flags (`razpravljalnica-node`):
- `--node-id <id>`: unique node identifier (required).
- `--listen <addr>`: address the node binds to (required).
- `--advertise <addr>`: address the node advertises to peers/clients (required; often same as listen).
- `--control-plane <addr>`: control-plane gRPC address (required to join/heartbeat).
- `--data-dir <path>` (if available): persistent state directory; defaults to in-memory or current working dir if not set.

## Cluster and control plane
- Show current cluster state:
  - `razcli cluster`
- Watch cluster state changes for a duration:
  - `razcli cluster --watch-for 30s`

## Node lifecycle
- Add a node as pending (returns current tail for bootstrapping):
  - `razcli node add --id n3 --addr 127.0.0.1:6003`
- Activate a previously added node (after it has synced):
  - `razcli node activate --id n3`

## Users
- Create user:
  - `razcli user --name alice`

## Topics
- Create topic:
  - `razcli topic create --name general`
- List topics (reads from tail):
  - `razcli topic list`

## Messages
- Post (head-only):
  - `razcli message post --topic 1 --user 1 --text "hello world"`
- Update (author only):
  - `razcli message update --topic 1 --user 1 --id 10 --text "new text"`
- Delete (author only):
  - `razcli message delete --topic 1 --user 1 --id 10`
- Like:
  - `razcli message like --topic 1 --user 2 --id 10`
- List messages (tail):
  - `razcli message list --topic 1 --from 0 --limit 50`

## Subscriptions (streaming)
- Stream message events for topics (uses head to assign a subscription node, then streams):
  - `razcli subscribe --topics 1,2 --user 1 --from 0`

## Execution order tips
1) Start control plane (e.g., `go run ./cmd/razpravljalnica-control-plane --chain n1`), then start node(s) (e.g., `go run ./cmd/razpravljalnica-node --node-id n1 --listen 127.0.0.1:6001 --advertise 127.0.0.1:6001 --control-plane 127.0.0.1:50050`).
2) Add and activate new nodes via `razcli node add` then `razcli node activate` after sync.
3) Use `razcli user` and `razcli topic create/list` to set up content.
4) Use `razcli message` commands to post/update/delete/like and list.
5) Use `razcli subscribe` to watch live events.

## Source reference
- CLI implementation: [cmd/razcli/main.go](cmd/razcli/main.go)
- Client helpers used by CLI: [client/client.go](client/client.go)
- Service definitions: [protos/razpravljalnica.proto](protos/razpravljalnica.proto)
