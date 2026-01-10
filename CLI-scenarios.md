# CLI scenario recipes
Chains of commands to exercise features end-to-end. Assumes binaries built (or use `go run` equivalents) and default control plane address `127.0.0.1:50050`.

## Flag reference (razcli)
- `--control-plane <addr>`: control-plane gRPC address (default `127.0.0.1:50050`).
- `--addr <addr>`: force data-plane RPCs to a specific node (reads/writes/subs) instead of letting control-plane pick.
- `--no-redirect`: make writes fail instead of redirecting when hitting a non-head node (demo/test failures).
- `--timeout <duration>`: per-RPC timeout (default `5s`).
- `--watch-for <duration>`: stream cluster changes for the given duration (used with `cluster --watch-for`).

Control-plane flags (`razpravljalnica-control-plane`):
- `--chain <id[,id2,...]>`: initial chain node IDs (comma-separated). Required.
- `--listen <addr>` (if present): control-plane listen address (default `127.0.0.1:50050`).

Node flags (`razpravljalnica-node`):
- `--node-id <id>`: unique node identifier (required).
- `--listen <addr>`: address the node binds to (required).
- `--advertise <addr>`: address the node advertises to peers/clients (required; often same as listen).
- `--control-plane <addr>`: control-plane gRPC address to join/heartbeat (required).
- `--data-dir <path>` (if available): persistent state directory; defaults to in-memory or CWD if not set.

## Start control plane and nodes
```
# control plane (choose seed IDs for the chain)
go run ./cmd/razpravljalnica-control-plane --chain n1

# node n1 (head/tail when alone)
go run ./cmd/razpravljalnica-node \
	--node-id n1 \
	--listen 127.0.0.1:6001 \
	--advertise 127.0.0.1:6001 \
	--control-plane 127.0.0.1:50050

# add n2 to the cluster and start it
bin/razcli node add --id n2 --addr 127.0.0.1:6002
go run ./cmd/razpravljalnica-node \
	--node-id n2 \
	--listen 127.0.0.1:6002 \
	--advertise 127.0.0.1:6002 \
	--control-plane 127.0.0.1:50050
```

Targeting specific nodes:
- Add `--addr <nodeAddr>` to force data-plane RPCs to a node (reads/writes/subs). The control-plane address remains `--control-plane`.
- Add `--no-redirect` to suppress auto-redirects to head when writing; useful to demo failures on non-head nodes.

## 0) Build binaries (once)
```
go build -o bin/razcli ./cmd/razcli
```

## 1) Single-node happy path (head=tail)
```
# start control plane
Go run ./cmd/razpravljalnica-control-plane --chain n1

# start node n1 (head/tail)
Go run ./cmd/razpravljalnica-node --node-id n1 --listen 127.0.0.1:6001 --advertise 127.0.0.1:6001 --control-plane 127.0.0.1:50050

# sanity: cluster state
bin/razcli cluster

# create user/topic, post, like, list
bin/razcli user --name alice
bin/razcli topic create --name general
bin/razcli message post --topic 1 --user 1 --text "hello"
bin/razcli message like --topic 1 --user 1 --id 1
bin/razcli message list --topic 1 --from 0 --limit 10
```

## 2) Add a new node (n2) — auto promotion
```
# add pending node
bin/razcli node add --id n2 --addr 127.0.0.1:6002

# start node n2 pointing at control plane; it will auto-promote when caught up via heartbeats
go run ./cmd/razpravljalnica-node --node-id n2 --listen 127.0.0.1:6002 --advertise 127.0.0.1:6002 --control-plane 127.0.0.1:50050

# watch state
bin/razcli cluster --watch-for 5s
```

## 3) Write on head, read on tail (two-node chain)
```
# post via head (n1)
bin/razcli message post --topic 1 --user 1 --text "from head"

# list via tail (n2)
bin/razcli message list --topic 1 --from 0 --limit 20

# optionally force read against head or middle to demo forwarding/dirty handling
# bin/razcli --addr 127.0.0.1:6001 message list --topic 1 --from 0 --limit 20
```

## 4) Update/delete permissions check
```
# create another user
bin/razcli user --name bob

# bob attempts to update alice's message (should fail)
bin/razcli message update --topic 1 --user 2 --id 1 --text "hijack"

# alice updates her own message
bin/razcli message update --topic 1 --user 1 --id 1 --text "edited by alice"

# alice deletes her message
bin/razcli message delete --topic 1 --user 1 --id 1
```

## 5) Subscription stream
```
# start a subscription (blocks; run in separate terminal)
bin/razcli subscribe --topics 1 --user 1 --from 0

# in another terminal, produce events
bin/razcli message post --topic 1 --user 1 --text "sub event 1"
bin/razcli message like --topic 1 --user 1 --id 2

# to force subscription via a specific node (e.g., n2) and see assignment/commit logs
# bin/razcli --addr 127.0.0.1:6002 subscribe --topics 1 --user 1 --from 0
```

## 6) Topic listing and pagination
```
# create several topics
bin/razcli topic create --name t1
bin/razcli topic create --name t2
bin/razcli topic create --name t3

# list topics
bin/razcli topic list

# paginate messages
bin/razcli message list --topic 1 --from 0 --limit 2
bin/razcli message list --topic 1 --from 2 --limit 2
```

## 7) Failure/activation retry path
```
# watch cluster
bin/razcli cluster --watch-for 10s

# stop node n2 (simulate failure)
# (press Ctrl+C in the terminal running n2)

# observe tail moves back to n1 in the watch output

# restart n2; it will auto-promote after catch-up
go run ./cmd/razpravljalnica-node --node-id n2 --listen 127.0.0.1:6002 --advertise 127.0.0.1:6002 --control-plane 127.0.0.1:50050

# to demo failed write against non-head
# bin/razcli --addr 127.0.0.1:6002 --no-redirect message post --topic 1 --user 1 --text "should fail (not head)"
```

Notes:
- Replace `Go run ...` with `go run` (lowercase) or prebuilt binaries as preferred.
- For production-like runs, keep control-plane and nodes in separate terminals.
- Adjust addresses/IDs to match your environment.
