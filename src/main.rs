use std::env;
use std::path::{Path, PathBuf};

use mstu_engine::app::App;
use mstu_engine::rpc;
use mstu_engine::{log_error, log_info, log_warn, logging};

/// How far up the tree a `plugins` folder is looked for.
const SEARCH_DEPTH: usize = 4;

/// Where the plugins were collected: `MSTU_PLUGIN_DIR`, or the nearest
/// `plugins` folder from here upwards.
///
/// Plugins are built in their own repositories and collected into one folder,
/// which is rarely the one the engine is run from.
fn plugin_dir() -> PathBuf {
    if let Ok(dir) = env::var("MSTU_PLUGIN_DIR") {
        return PathBuf::from(dir);
    }

    let here = env::current_dir().unwrap_or_else(|_| PathBuf::from("."));

    here.ancestors()
        .take(SEARCH_DEPTH)
        .map(|at| at.join("plugins"))
        .find(|dir| dir.is_dir())
        .unwrap_or_else(|| here.join("plugins"))
}

/// Loads every plugin library in `dir`, keyed by file stem without `lib`.
fn load_plugins(app: &mut App, dir: &Path) -> usize {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) => {
            log_warn!("no plugins loaded, '{}' cannot be read: {error}", dir.display());
            return 0;
        }
    };

    let mut loaded = 0;

    for entry in entries.flatten() {
        let path = entry.path();

        if path.extension().and_then(|ext| ext.to_str()) != Some("so") {
            continue;
        }

        let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
            continue;
        };

        let key = stem.strip_prefix("lib").unwrap_or(stem).replace('_', "-");

        match app.load(&key, &path.to_string_lossy()) {
            Ok(()) => loaded += 1,
            // Not every .so in the folder is a plugin.
            Err(error) => log_warn!("skipped {}: {}", path.display(), error),
        }
    }

    loaded
}

#[tokio::main]
async fn main() -> Result<(), String> {
    logging::init();

    log_info!("MSTU started");

    if env::args().any(|arg| arg == "--serve") {
        let dir = plugin_dir();

        let mut app = App::new();
        let loaded = load_plugins(&mut app, &dir);

        log_info!("serving {loaded} plugin librarie(s) from '{}'", dir.display());

        let served = rpc::serve(app).await.map_err(|error| error.to_string());

        if let Err(error) = &served {
            log_error!("{error}");
        }

        return served;
    }

    log_info!("MSTU stopped");

    Ok(())
}
