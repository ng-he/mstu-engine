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
[mstu-sdk](https://github.com/ng-he/mstu-sdk). Every library in the plugin
folder is tried, and anything without that symbol is skipped with a warning.

A plugin may link against more than the engine does -- mstu-image-decoder
links against libav -- and `src/library.rs` is where that is made to work. On
Linux and macOS those libraries come from wherever the system keeps its own.
Windows has nowhere to install them, and its loader looks beside the running
exe rather than beside the library that asked, so the plugin's own folder is
added to the search path and a plugin's dependencies travel beside it in the
plugin folder; `collect.sh` carries across whatever a plugin leaves in its
`dist/`. A machine wide install stays usable: a plugin that fails to load that
way is tried once more with the ordinary search, PATH included.
