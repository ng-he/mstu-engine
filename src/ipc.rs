//! The local channel the desktop app talks to the engine over.
//!
//! A unix socket where there are unix sockets, a named pipe on Windows. Both
//! are local to the machine and private to the user's session, which a port on
//! loopback would not be; what travels over them is the same line protocol
//! either way, so only binding and accepting differ.

use std::env;
use std::io;

#[cfg(windows)]
pub use pipe::{Listener, Stream};
#[cfg(unix)]
pub use socket::{Listener, Stream};

/// What to bind, or to connect to: `MSTU_SOCKET` if it was asked for.
///
/// Two engines on one machine would otherwise fight over the same name, so
/// anything that wants its own engine — the integration tests, a second app —
/// names it here.
pub fn address() -> String {
    env::var("MSTU_SOCKET").unwrap_or_else(|_| default_address())
}

/// Where the desktop app looks without being told.
#[cfg(unix)]
fn default_address() -> String {
    let dir = env::var("XDG_RUNTIME_DIR").unwrap_or_else(|_| "/tmp".to_string());

    std::path::Path::new(&dir)
        .join("mstu.sock")
        .to_string_lossy()
        .into_owned()
}

#[cfg(windows)]
fn default_address() -> String {
    // Pipes live in their own namespace, so there is no runtime directory to
    // find and nothing to clean up afterwards.
    r"\\.\pipe\mstu".to_string()
}

fn already_serving(address: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::AddrInUse,
        format!("an engine is already serving on {address}"),
    )
}

#[cfg(unix)]
mod socket {
    use std::io;

    use tokio::net::{UnixListener, UnixStream};

    pub type Stream = UnixStream;

    pub struct Listener(UnixListener);

    impl Listener {
        pub async fn bind(address: &str) -> io::Result<Self> {
            // Taking the path from a live engine would orphan its listener,
            // leaving it running but unreachable. Only a stale socket may go.
            if UnixStream::connect(address).await.is_ok() {
                return Err(super::already_serving(address));
            }

            let _ = std::fs::remove_file(address);

            Ok(Self(UnixListener::bind(address)?))
        }

        pub async fn accept(&mut self) -> io::Result<Stream> {
            Ok(self.0.accept().await?.0)
        }
    }
}

#[cfg(windows)]
mod pipe {
    use std::io;

    use tokio::net::windows::named_pipe::{NamedPipeServer, ServerOptions};

    pub type Stream = NamedPipeServer;

    /// One waiting pipe instance, replaced by the next as each client takes it.
    pub struct Listener {
        address: String,
        waiting: NamedPipeServer,
    }

    impl Listener {
        pub async fn bind(address: &str) -> io::Result<Self> {
            // Asking for the first instance is itself the check a unix socket
            // has to make by hand: a second engine cannot have the name.
            let waiting = ServerOptions::new()
                .first_pipe_instance(true)
                .create(address)
                .map_err(|error| match error.kind() {
                    io::ErrorKind::PermissionDenied => super::already_serving(address),
                    _ => error,
                })?;

            Ok(Self {
                address: address.to_string(),
                waiting,
            })
        }

        pub async fn accept(&mut self) -> io::Result<Stream> {
            self.waiting.connect().await?;

            // The instance a client holds is its own; the next one waits.
            let next = ServerOptions::new().create(&self.address)?;

            Ok(std::mem::replace(&mut self.waiting, next))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::fake;

    #[test]
    fn what_was_asked_for_is_what_is_served() {
        let _globals = fake::globals();

        let was = env::var("MSTU_SOCKET").ok();

        unsafe { env::set_var("MSTU_SOCKET", "a-name-of-its-own") };
        assert_eq!(address(), "a-name-of-its-own");

        unsafe { env::remove_var("MSTU_SOCKET") };
        assert_ne!(address(), "a-name-of-its-own");

        if let Some(was) = was {
            unsafe { env::set_var("MSTU_SOCKET", was) };
        }
    }

    /// The name the desktop app expects, platform by platform.
    #[test]
    fn the_default_is_the_one_the_app_looks_for() {
        if cfg!(unix) {
            assert!(default_address().ends_with("mstu.sock"));
        } else {
            assert_eq!(default_address(), r"\\.\pipe\mstu");
        }
    }
}
