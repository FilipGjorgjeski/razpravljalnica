# CLI scenario recipes
Chains of commands to exercise features end-to-end. Assumes binaries built (or use `go run` equivalents) and default control plane address `127.0.0.1:50050`.

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
