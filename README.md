# razpravljalnica

## CLI and server docs

- [CLI.md](CLI.md)

- [CLI-scenarios.md](CLI-scenarios.md)

## GUI Client

Launch with

```
go run cmd/razpravljalnica-gui-client/main.go
```

or

```
make run-gui
```

use the `--control-plane=:50050` command to specify a custom control plane address.

### Controls

Navigation via arrow keys

`Enter` to select the highlighted entry. To edit a message, select it from the list

`Tab` to select different form fields

`N` to create a new topic while in topic menu or to write a new message while in message list

`L` to like a message

`Delete` to delete your message

`Ctrl+R` forces a UI update, debugging only

```

```
