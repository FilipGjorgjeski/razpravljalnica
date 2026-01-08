# razpravljalnica CLI guide

This document shows how to run every implemented RPC via the `razcli` command-line tool. The CLI talks to the control plane and the data-plane nodes using gRPC.

## Prerequisites
- Go installed and `$GOBIN` on PATH.
- From the repo root, build the CLI (or run with `go run`):
  - Build: `go build -o bin/razcli ./cmd/razcli`
  - Run in place: `go run ./cmd/razcli --help`
- Control plane and nodes must be running and reachable. Default control-plane address: `127.0.0.1:50050`.

Global flags (apply to all commands):
- `--control-plane <addr>`: control plane gRPC address (default `127.0.0.1:50050`).
- `--timeout <duration>`: per-RPC timeout (default `5s`).

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
