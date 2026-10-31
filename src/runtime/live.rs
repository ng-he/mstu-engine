use std::{
    collections::HashMap,
    sync::{LazyLock, Mutex, MutexGuard},
};

use mstu_sdk::{Message, Str};
use tokio::sync::mpsc::UnboundedSender;

use crate::{
    log_trace, log_warn,
    runtime::{
        owned::{Owned, own_message},
        schema::{self, Shape},
    },
};

/// What a plugin last published, and the shape it must publish.
struct Entry {
    shape: Vec<Shape>,
    values: Vec<Owned>,
}

/// The latest live values of every plugin: readings are state, so the last
/// one is kept for whoever looks next.
pub struct Board {
    plugins: HashMap<String, Entry>,

    /// Woken once when a reading moves, until the news is taken.
    tap: Option<UnboundedSender<()>>,
    moved: bool,
}

impl Board {
    fn new() -> Self {
        Self {
            plugins: HashMap::new(),
            tap: None,
            moved: false,
        }
    }

    /// Opens the board a plugin publishes on, before it is created.
    pub fn register(&mut self, plugin_id: &str, shape: Vec<Shape>) {
        if shape.is_empty() {
            return;
        }

        self.plugins.insert(
            plugin_id.to_string(),
            Entry {
                shape,
                values: Vec::new(),
            },
        );
    }

    pub fn forget(&mut self, plugin_id: &str) {
        self.plugins.remove(plugin_id);
    }

    /// Wakes `tap` when a reading moves, `None` stops it.
    pub fn set_tap(&mut self, tap: Option<UnboundedSender<()>>) {
        self.tap = tap;
    }

    /// Takes a snapshot, copied out before the ABI call returns.
    pub fn publish(&mut self, plugin_id: &str, snapshot: &Message) {
        let Some(entry) = self.plugins.get_mut(plugin_id) else {
            log_warn!("'{plugin_id}' published live values but declares no live schema");
            return;
        };

        // A reading the UI cannot read by its schema is worse than none.
        if !schema::matches(snapshot, &entry.shape) {
            log_warn!("'{plugin_id}' live {snapshot:?} does not match its live schema");
            return;
        }

        log_trace!("'{plugin_id}' live {snapshot:?}");

        entry.values = own_message(snapshot);

        // One wake covers every publish until the news is taken, so a plugin
        // publishing per frame does not wake the socket per frame.
        if !self.moved {
            self.moved = true;

            if let Some(tap) = self.tap.as_ref() {
                let _ = tap.send(());
            }
        }
    }

    /// Readings that are not what `sent` last reported, with `sent` brought
    /// up to date: an idle plugin republishes the same numbers forever.
    pub fn changed(&mut self, sent: &mut HashMap<String, Vec<Owned>>) -> Vec<(String, Vec<Owned>)> {
        self.moved = false;

        // Gone plugins stop being remembered, so one that comes back reports.
        sent.retain(|id, _| self.plugins.contains_key(id));

        let mut news = Vec::new();

        for (id, entry) in self.plugins.iter() {
            // Empty means the plugin has said nothing, not that it reads zero.
            if entry.values.is_empty() {
                continue;
            }

            if sent.get(id).is_some_and(|last| *last == entry.values) {
                continue;
            }

            sent.insert(id.clone(), entry.values.clone());
            news.push((id.clone(), entry.values.clone()));
        }

        news
    }
}

static BOARD: LazyLock<Mutex<Board>> = LazyLock::new(|| Mutex::new(Board::new()));

pub fn board() -> MutexGuard<'static, Board> {
    BOARD.lock().unwrap()
}

/// `HostContext::publish_live`.
pub extern "C" fn publish_live(plugin_id: Str, snapshot: Message) {
    board().publish(unsafe { plugin_id.as_str() }, &snapshot);
}

#[cfg(test)]
mod tests {
    use std::vec;

    use mstu_sdk::{Slice, Value, ValueKind};

    use super::*;

    fn shape() -> Vec<Shape> {
        let plain = |kind| Shape {
            kind,
            fields: None,
            element: None,
        };

        vec![plain(ValueKind::ValueUint), plain(ValueKind::ValueBool)]
    }

    fn message(values: &[Value]) -> Message {
        Message {
            values: Slice::from_raw_parts(values.as_ptr(), values.len()),
        }
    }

    #[test]
    fn keeps_the_last_reading_for_whoever_looks_next() {
        let mut board = Board::new();
        board.register("a", shape());

        let values = [7u64.into(), true.into()];
        board.publish("a", &message(&values));

        let mut sent = HashMap::new();
        assert_eq!(board.changed(&mut sent).len(), 1);

        // Nothing moved since, so there is no news.
        assert!(board.changed(&mut sent).is_empty());
    }

    #[test]
    fn refuses_a_reading_of_the_wrong_shape() {
        let mut board = Board::new();
        board.register("a", shape());

        let values = [7u64.into()];
        board.publish("a", &message(&values));

        assert!(board.changed(&mut HashMap::new()).is_empty());
    }

    #[test]
    fn a_forgotten_plugin_reports_again_when_it_comes_back() {
        let mut board = Board::new();
        board.register("a", shape());

        let values = [7u64.into(), true.into()];
        board.publish("a", &message(&values));

        let mut sent = HashMap::new();
        board.changed(&mut sent);

        board.forget("a");
        assert!(board.changed(&mut sent).is_empty());

        board.register("a", shape());
        board.publish("a", &message(&values));

        assert_eq!(board.changed(&mut sent).len(), 1);
    }
}

#[cfg(test)]
mod more_tests {
    use std::collections::HashMap;

    use mstu_sdk::{Slice, Value, ValueKind};
    use tokio::sync::mpsc::unbounded_channel;

    use super::*;

    fn shape() -> Vec<Shape> {
        vec![Shape {
            kind: ValueKind::ValueUint,
            fields: None,
            element: None,
        }]
    }

    fn message(values: &[Value]) -> Message {
        Message {
            values: Slice::from_raw_parts(values.as_ptr(), values.len()),
        }
    }

    #[test]
    fn a_plugin_with_nothing_to_show_is_not_on_the_board() {
        let mut board = Board::new();

        board.register("a", Vec::new());
        board.publish("a", &message(&[1u64.into()]));

        assert!(board.changed(&mut HashMap::new()).is_empty());
    }

    /// One wake carries every publish until the news is taken, so a plugin
    /// publishing per frame does not wake the socket per frame.
    #[test]
    fn many_readings_wake_the_socket_once() {
        let (tap, mut woken) = unbounded_channel();

        let mut board = Board::new();
        board.set_tap(Some(tap));
        board.register("a", shape());

        for value in 0..10u64 {
            board.publish("a", &message(&[value.into()]));
        }

        let mut wakes = 0;
        while woken.try_recv().is_ok() {
            wakes += 1;
        }

        assert_eq!(wakes, 1);

        // Once the news is taken, the next reading wakes it again.
        board.changed(&mut HashMap::new());
        board.publish("a", &message(&[11u64.into()]));

        assert!(woken.try_recv().is_ok());
    }

    #[test]
    fn nothing_is_woken_once_the_tap_is_taken_away() {
        let (tap, mut woken) = unbounded_channel();

        let mut board = Board::new();
        board.set_tap(Some(tap));
        board.register("a", shape());
        board.set_tap(None);

        board.publish("a", &message(&[1u64.into()]));

        assert!(woken.try_recv().is_err());
    }

    /// `HostContext::publish_live`, as a plugin calls it.
    #[test]
    fn a_plugin_publishes_through_the_host_onto_the_board() {
        let _globals = crate::runtime::fake::globals();

        board().register("PLUG-LIVE-TEST", shape());

        let values = [42u64.into()];
        publish_live(Str::from_static("PLUG-LIVE-TEST"), message(&values));

        let news = board().changed(&mut HashMap::new());
        let ours = news
            .iter()
            .find(|(plugin, _)| plugin == "PLUG-LIVE-TEST")
            .expect("what was just published");

        assert_eq!(ours.1, vec![crate::runtime::owned::Owned::Uint(42)]);

        board().forget("PLUG-LIVE-TEST");
    }

    #[test]
    fn the_latest_reading_is_the_one_that_is_sent() {
        let mut board = Board::new();
        board.register("a", shape());

        board.publish("a", &message(&[1u64.into()]));
        board.publish("a", &message(&[2u64.into()]));

        let news = board.changed(&mut HashMap::new());

        assert_eq!(news.len(), 1);
        assert_eq!(news[0].1, vec![crate::runtime::owned::Owned::Uint(2)]);
    }
}
