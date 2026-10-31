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
    let here = env::current_dir().unwrap_or_else(|_| PathBuf::from("."));

    plugin_dir_from(&here, env::var("MSTU_PLUGIN_DIR").ok())
}

/// Where `plugin_dir` decides, given a place to start and what was asked for.
fn plugin_dir_from(here: &Path, configured: Option<String>) -> PathBuf {
    if let Some(dir) = configured {
        return PathBuf::from(dir);
    }

    here.ancestors()
        .take(SEARCH_DEPTH)
        .map(|at| at.join("plugins"))
        .find(|dir| dir.is_dir())
        .unwrap_or_else(|| here.join("plugins"))
}

/// The name a library in the plugins folder is known by, or nothing if the
/// file is no library of this platform's kind.
///
/// Cargo names it `libmstu_media_dumper.so` here and `mstu_media_dumper.dll`
/// on Windows, and either way the plugin is `mstu-media-dumper`.
fn plugin_key(path: &Path) -> Option<String> {
    if path.extension()? != env::consts::DLL_EXTENSION {
        return None;
    }

    let stem = path.file_stem()?.to_str()?;

    Some(stem.strip_prefix("lib").unwrap_or(stem).replace('_', "-"))
}

/// Loads every plugin library in `dir`, keyed as `plugin_key` says.
fn load_plugins(app: &mut App, dir: &Path) -> usize {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) => {
            log_warn!(
                "no plugins loaded, '{}' cannot be read: {error}",
                dir.display()
            );
            return 0;
        }
    };

    let mut loaded = 0;

    for entry in entries.flatten() {
        let path = entry.path();

        let Some(key) = plugin_key(&path) else {
            continue;
        };

        match app.load(&key, &path.to_string_lossy()) {
            Ok(()) => loaded += 1,
            // Not every library in the folder is a plugin.
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

        log_info!(
            "serving {loaded} plugin librarie(s) from '{}'",
            dir.display()
        );

        let served = rpc::serve(app).await.map_err(|error| error.to_string());

        if let Err(error) = &served {
            log_error!("{error}");
        }

        return served;
    }

    log_info!("MSTU stopped");

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    /// A tree of its own, taken away afterwards.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new(name: &str) -> Self {
            let path = env::temp_dir().join(format!("mstu-main-{}-{name}", std::process::id()));

            let _ = fs::remove_dir_all(&path);
            fs::create_dir_all(&path).expect("a directory to look in");

            Self(path)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn what_was_asked_for_wins() {
        let asked = plugin_dir_from(Path::new("/anywhere"), Some("/opt/plugins".to_string()));

        assert_eq!(asked, PathBuf::from("/opt/plugins"));
    }

    #[test]
    fn the_folder_beside_the_engine_is_found_from_within_it() {
        let root = Scratch::new("beside");
        let plugins = root.0.join("plugins");

        fs::create_dir_all(&plugins).expect("a plugins folder");
        fs::create_dir_all(root.0.join("mstu-engine/src")).expect("an engine beside it");

        // Started from inside the engine's own repository, as cargo run does.
        let from = root.0.join("mstu-engine/src");

        assert_eq!(plugin_dir_from(&from, None), plugins);
    }

    #[test]
    fn the_nearest_one_is_the_one_taken() {
        let root = Scratch::new("nearest");
        let near = root.0.join("inner/plugins");

        fs::create_dir_all(&near).expect("the near folder");
        fs::create_dir_all(root.0.join("plugins")).expect("the far folder");

        assert_eq!(plugin_dir_from(&root.0.join("inner"), None), near);
    }

    /// Four levels up and no further, so a stray folder near the root of the
    /// disk is never taken for this project's.
    #[test]
    fn the_search_does_not_go_on_forever() {
        let root = Scratch::new("depth");
        let deep = root.0.join("a/b/c/d/e");

        fs::create_dir_all(&deep).expect("somewhere deep");
        fs::create_dir_all(root.0.join("plugins")).expect("a folder far above it");

        assert_eq!(plugin_dir_from(&deep, None), deep.join("plugins"));
    }

    /// However cargo named it here, the plugin is known by its own name.
    #[test]
    fn a_library_is_known_by_the_plugin_it_holds() {
        let ext = env::consts::DLL_EXTENSION;

        assert_eq!(
            plugin_key(Path::new(&format!("plugins/libmstu_media_dumper.{ext}"))),
            Some("mstu-media-dumper".to_string())
        );

        // What Windows builds, where nothing carries the prefix.
        assert_eq!(
            plugin_key(Path::new(&format!("plugins/mstu_media_dumper.{ext}"))),
            Some("mstu-media-dumper".to_string())
        );
    }

    #[test]
    fn what_is_not_a_library_is_passed_over() {
        assert_eq!(plugin_key(Path::new("plugins/mstu-media-dumper-ui")), None);
        assert_eq!(plugin_key(Path::new("plugins/notes.txt")), None);

        // A library, but not one this platform can load.
        let foreign = if cfg!(windows) {
            "plugin.so"
        } else {
            "plugin.dll"
        };
        assert_eq!(plugin_key(Path::new(foreign)), None);
    }

    #[test]
    fn with_nothing_to_find_it_names_where_it_looked() {
        let root = Scratch::new("nothing");

        assert_eq!(plugin_dir_from(&root.0, None), root.0.join("plugins"));
    }
}
