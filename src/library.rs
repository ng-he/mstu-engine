//! Opening a plugin library, and finding what that library needs in turn.
//!
//! A plugin may link against more than the engine does — mstu-image-decoder
//! links against libav — and those libraries have to be found when the plugin
//! is loaded, not when the engine was built. Where the loader looks for them
//! is the one thing the platforms do not agree on.

use libloading::Library;

/// Opens a plugin library.
///
/// A dependency comes from where the system keeps its libraries, which is
/// where a package manager put it; nothing extra to say.
///
/// # Safety
///
/// Loading a library runs its initialisers, and unloading runs its
/// terminators; both are arbitrary foreign code.
#[cfg(not(windows))]
pub unsafe fn open(path: &str) -> Result<Library, libloading::Error> {
    unsafe { Library::new(path) }
}

/// Opens a plugin library, with the plugin's own folder on the search path.
///
/// Windows has no system library folder to install into, so a plugin carries
/// its dependencies beside itself — and the loader looks beside the running
/// exe, never beside the library that asked, until it is told to.
///
/// # Safety
///
/// Loading a library runs its initialisers, and unloading runs its
/// terminators; both are arbitrary foreign code.
#[cfg(windows)]
pub unsafe fn open(path: &str) -> Result<Library, libloading::Error> {
    use std::path::PathBuf;

    use libloading::os::windows::{
        LOAD_LIBRARY_SEARCH_DEFAULT_DIRS, LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR, Library as Windows,
    };

    // The flag that adds that folder takes an absolute path and nothing else.
    let full: PathBuf = std::path::absolute(path).unwrap_or_else(|_| PathBuf::from(path));
    let flags = LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR | LOAD_LIBRARY_SEARCH_DEFAULT_DIRS;

    match unsafe { Windows::load_with_flags(&full, flags) } {
        Ok(library) => Ok(library.into()),

        // These flags replace the whole search order, PATH included, and a
        // machine wide ffmpeg install is on PATH; so one plain attempt follows.
        Err(_) => unsafe { Library::new(&full) },
    }
}
