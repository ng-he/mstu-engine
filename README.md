# mstu-engine

The pipeline engine: it loads plugins, runs each node on its own task, routes
messages between them, and serves a JSON line protocol over a unix socket that
the desktop app talks to.

## Running

```sh
cargo run --release -- --serve
```

It reads plugins from `./plugins`, or from `MSTU_PLUGIN_DIR` when that is set:

```sh
MSTU_PLUGIN_DIR=../plugins cargo run --release -- --serve
```

The socket lands at `$XDG_RUNTIME_DIR/mstu.sock`, so a second engine can be run
beside the first with `XDG_RUNTIME_DIR` pointed somewhere else.

## Plugins

A plugin is a shared library exporting `plugin_descriptor`; see
[mstu-sdk](https://github.com/ng-he/mstu-sdk). Every `.so` in the plugin folder
is tried, and anything without that symbol is skipped with a warning.
