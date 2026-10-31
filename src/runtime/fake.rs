//! A plugin that does nothing, for testing the engine without loading one.
//!
//! Everything the engine asks of a plugin is answered here: schemas it can
//! build shapes from, an event it can publish, a command it can invoke. What
//! the command was called with is recorded per thread, since the tests run
//! beside each other and an event is published on the thread that raised it.

use std::cell::Cell;
use std::ptr;
use std::sync::{Mutex, MutexGuard};

use mstu_sdk::{
    CommandDescriptor, EventDescriptor, Field, HostContext, Message, Metadata, PluginDescriptor,
    PluginHandle, ProcessContext, Schema, Slice, Str, TYPE_STRING, TYPE_U64, Value, message, slice,
};

// { "id": u64, "name": string }, used as event payload, command payload and
// the shape a node takes and produces.
static FIELDS: [Field; 2] = [
    Field {
        name: Str::from_static("id"),
        ty: &TYPE_U64,
        description: Str::empty(),
    },
    Field {
        name: Str::from_static("name"),
        ty: &TYPE_STRING,
        description: Str::empty(),
    },
];

pub static PAYLOAD: Schema = Schema {
    fields: slice!(FIELDS),
};

static EVENTS: [EventDescriptor; 1] = [EventDescriptor {
    event_name: Str::from_static("happened"),
    schema: &PAYLOAD,
    description: Str::empty(),
}];

static COMMANDS: [CommandDescriptor; 1] = [CommandDescriptor {
    command_name: Str::from_static("do.it"),
    schema: &PAYLOAD,
    description: Str::empty(),
}];

thread_local! {
    static INVOKED: Cell<usize> = const { Cell::new(0) };
    static LAST_ID: Cell<u64> = const { Cell::new(0) };
}

/// How many times a command has been invoked on this thread.
pub fn invoked() -> usize {
    INVOKED.with(Cell::get)
}

/// The first field of the last payload invoked on this thread.
pub fn last_id() -> u64 {
    LAST_ID.with(Cell::get)
}

pub fn forget_calls() {
    INVOKED.with(|calls| calls.set(0));
}

/// Held by any test touching the engine's one board, manager or tap, since
/// the tests run beside each other and those are shared by the whole process.
static GLOBALS: Mutex<()> = Mutex::new(());

pub fn globals() -> MutexGuard<'static, ()> {
    // A test that failed holding this should not fail the rest.
    GLOBALS.lock().unwrap_or_else(|held| held.into_inner())
}

/// Handles are compared, never followed, so a number stands in for one.
pub fn handle(n: usize) -> PluginHandle {
    n as PluginHandle
}

extern "C" fn metadata() -> &'static Metadata {
    static META: Metadata = Metadata {
        name: Str::from_static("fake"),
        version: Str::from_static("0"),
    };

    &META
}

extern "C" fn create(_: *const HostContext, _: Str) -> PluginHandle {
    ptr::null_mut()
}

extern "C" fn id(_: PluginHandle) -> Str {
    Str::from_static("fake")
}

extern "C" fn release(_: PluginHandle) {}

extern "C" fn no_schema() -> *const Schema {
    ptr::null()
}

extern "C" fn payload_schema() -> *const Schema {
    &PAYLOAD
}

extern "C" fn settings(_: PluginHandle, _: *mut message::Writer) -> bool {
    true
}

extern "C" fn set_parameter(_: PluginHandle, _: usize, _: Value) -> bool {
    true
}

extern "C" fn ui() -> Str {
    Str::empty()
}

extern "C" fn process(_: PluginHandle, _: *mut ProcessContext) -> bool {
    true
}

extern "C" fn events() -> Slice<EventDescriptor> {
    slice!(EVENTS)
}

extern "C" fn commands() -> Slice<CommandDescriptor> {
    slice!(COMMANDS)
}

extern "C" fn invoke(_: PluginHandle, _: usize, input: *const Message) -> bool {
    INVOKED.with(|calls| calls.set(calls.get() + 1));

    let id = unsafe { &*input }
        .values_slice()
        .first()
        .and_then(|value| value.get::<u64>())
        .unwrap_or(0);

    LAST_ID.with(|last| last.set(id));

    true
}

/// Takes nothing and produces the payload, so the engine runs it as a source.
pub static SOURCE: PluginDescriptor = PluginDescriptor {
    metadata,
    create,
    id,
    release,
    process_input_schema: no_schema,
    process_output_schema: payload_schema,
    settings_schema: no_schema,
    settings,
    set_parameter,
    ui,
    process,
    events,
    commands,
    invoke,
    live_schema: no_schema,
};

/// Takes the payload and produces nothing, so it is wired to, never from.
pub static SINK: PluginDescriptor = PluginDescriptor {
    metadata,
    create,
    id,
    release,
    process_input_schema: payload_schema,
    process_output_schema: no_schema,
    settings_schema: no_schema,
    settings,
    set_parameter,
    ui,
    process,
    events,
    commands,
    invoke,
    live_schema: no_schema,
};
