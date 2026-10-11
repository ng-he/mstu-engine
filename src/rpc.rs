use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use mstu_sdk::{
    CommandDescriptor, EventDescriptor, Message, PluginDescriptor, Schema, Slice, Str, Type,
    TypeKind, Value, ValueKind,
};
use serde::Deserialize;
use serde_json::{Value as Json, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    sync::mpsc::{UnboundedReceiver, unbounded_channel},
};

use crate::{
    app::App,
    ipc, log_debug, log_info, log_trace, log_warn,
    runtime::{
        event::{self, Notice},
        live,
        mapper::{Mapper, Mapping},
        owned::Owned,
    },
};

/// How often queued pipeline output is routed while the socket is quiet.
const ROUTE_INTERVAL: Duration = Duration::from_millis(1);

/// How long routing waits once nothing has come through for a while. The
/// interval doubles up to this while the pipeline is idle and drops back to
/// `ROUTE_INTERVAL` the moment a message moves, so a quiet engine stops
/// waking a core a thousand times a second for nothing.
const ROUTE_IDLE_MAX: Duration = Duration::from_millis(20);

/// The closest together two live lines may go out. Plugins publish as often
/// as they like; a source publishing per frame must not become a frame of
/// socket traffic.
const LIVE_INTERVAL: Duration = Duration::from_millis(250);

/// However quiet everything is, the whole picture goes out this often: a page
/// that has just opened should not have to wait for a reading to move.
const LIVE_REFRESH: Duration = Duration::from_secs(2);

#[derive(Deserialize)]
struct Request {
    id: u64,
    method: String,

    #[serde(default)]
    params: Json,
}

// ---------- describing plugins ----------

fn type_name(ty: &Type) -> &'static str {
    match ty.kind {
        TypeKind::TypeBool => "bool",
        TypeKind::TypeInt => {
            if unsafe { ty.schema.int_ }.signed {
                "int"
            } else {
                "uint"
            }
        }
        TypeKind::TypeFloat => "float",
        TypeKind::TypeString => "string",
        TypeKind::TypeRecord => "record",
        TypeKind::TypeEnum => "enum",
        TypeKind::TypeList => "list",
        TypeKind::TypeBytes => "bytes",
    }
}

/// How far a schema is described before it is cut off, in case a record ever
/// holds itself.
const MAX_SCHEMA_DEPTH: usize = 8;

/// A scalar as JSON, for the literals enum variants carry.
fn value_json(value: &Value) -> Json {
    match value.kind {
        ValueKind::ValueBool => json!(unsafe { value.data.bool_ }),
        ValueKind::ValueInt => json!(unsafe { value.data.int_ }),
        ValueKind::ValueUint => json!(unsafe { value.data.uint_ }),
        ValueKind::ValueFloat => json!(unsafe { value.data.float_ }),
        ValueKind::ValueString => json!(unsafe { value.data.string_.as_str() }),
        _ => Json::Null,
    }
}

/// Everything a type says about itself: how wide a number is, what an enum
/// admits, and the fields of a record, so a client can show the whole shape.
fn type_json(ty: &Type, depth: usize) -> Json {
    let mut described = json!({ "type": type_name(ty) });

    match ty.kind {
        TypeKind::TypeInt => {
            let int = unsafe { ty.schema.int_ };

            described["bits"] = json!(int.bits);
            described["signed"] = json!(int.signed);
        }

        TypeKind::TypeFloat => {
            described["bits"] = json!(unsafe { ty.schema.float_ }.bits);
        }

        TypeKind::TypeEnum => {
            let variants = unsafe { ty.schema.enum_.variants.as_slice() };

            described["variants"] = Json::Array(
                variants
                    .iter()
                    .map(|variant| {
                        json!({
                            "name": unsafe { variant.name.as_str() },
                            "value": value_json(&variant.value),
                        })
                    })
                    .collect(),
            );
        }

        TypeKind::TypeRecord if depth < MAX_SCHEMA_DEPTH => {
            described["fields"] = fields_json(unsafe { ty.schema.record_ }.schema, depth + 1);
        }

        TypeKind::TypeList if depth < MAX_SCHEMA_DEPTH => {
            described["element"] = type_json(unsafe { ty.schema.list_ }.element, depth + 1);
        }

        _ => {}
    }

    described
}

fn fields_json(schema: &Schema, depth: usize) -> Json {
    Json::Array(
        unsafe { schema.fields.as_slice() }
            .iter()
            .map(|field| {
                let mut described = type_json(field.ty, depth);

                described["name"] = json!(unsafe { field.name.as_str() });
                described["description"] = json!(unsafe { field.description.as_str() });

                described
            })
            .collect(),
    )
}

fn schema_json(schema: *const Schema) -> Json {
    if schema.is_null() {
        return Json::Null;
    }

    json!({ "fields": fields_json(unsafe { &*schema }, 0) })
}

fn events_json(events: Slice<EventDescriptor>) -> Json {
    let events = unsafe { events.as_slice() };

    Json::Array(
        events
            .iter()
            .map(|event| {
                json!({
                    "name": unsafe { event.event_name.as_str() },
                    "schema": schema_json(event.schema),
                })
            })
            .collect(),
    )
}

fn commands_json(commands: Slice<CommandDescriptor>) -> Json {
    let commands = unsafe { commands.as_slice() };

    Json::Array(
        commands
            .iter()
            .map(|command| {
                json!({
                    "name": unsafe { command.command_name.as_str() },
                    "schema": schema_json(command.schema),
                })
            })
            .collect(),
    )
}

fn library_json(key: &str, descriptor: &'static PluginDescriptor) -> Json {
    let metadata = (descriptor.metadata)();

    json!({
        "key": key,
        "name": unsafe { metadata.name.as_str() },
        "version": unsafe { metadata.version.as_str() },
        "source": (descriptor.process_input_schema)().is_null(),
        "input": schema_json((descriptor.process_input_schema)()),
        "output": schema_json((descriptor.process_output_schema)()),
        "settings": schema_json((descriptor.settings_schema)()),
        "live": schema_json((descriptor.live_schema)()),
        "events": events_json((descriptor.events)()),
        "commands": commands_json((descriptor.commands)()),
    })
}

fn owned_json(value: &Owned) -> Json {
    match value {
        Owned::None => Json::Null,
        Owned::Bool(value) => json!(value),
        Owned::Int(value) => json!(value),
        Owned::Uint(value) => json!(value),
        Owned::Float(value) => json!(value),
        Owned::Str(value) => json!(value),
        Owned::Bytes(len) => json!({ "bytes": len }),
        Owned::List(values) => Json::Array(values.iter().map(owned_json).collect()),
    }
}

// ---------- reading params ----------

/// Owns the strings a `Value` borrows for the length of one call.
#[derive(Default)]
struct Storage {
    strings: Vec<String>,
}

impl Storage {
    fn value(&mut self, json: &Json) -> Result<Value, String> {
        Ok(match json {
            Json::Null => Value::none(),
            Json::Bool(value) => (*value).into(),
            Json::String(value) => {
                self.strings.push(value.clone());
                Str::new(self.strings.last().unwrap().as_str()).into()
            }
            Json::Number(value) => {
                if let Some(value) = value.as_u64() {
                    value.into()
                } else if let Some(value) = value.as_i64() {
                    value.into()
                } else {
                    value.as_f64().unwrap_or_default().into()
                }
            }
            _ => return Err(format!("cannot pass {json} across the ABI")),
        })
    }
}

fn field(params: &Json, name: &str) -> Result<Json, String> {
    params
        .get(name)
        .cloned()
        .ok_or_else(|| format!("missing '{name}'"))
}

fn as_str(params: &Json, name: &str) -> Result<String, String> {
    field(params, name)?
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| format!("'{name}' must be a string"))
}

fn as_usize(params: &Json, name: &str) -> Result<usize, String> {
    field(params, name)?
        .as_u64()
        .map(|value| value as usize)
        .ok_or_else(|| format!("'{name}' must be a number"))
}

/// Mappings arrive as `[[from, to], ...]` field index pairs.
fn as_mapper(params: &Json, name: &str) -> Result<Mapper, String> {
    let mut mapper = Mapper::new();

    let Some(pairs) = params.get(name).and_then(Json::as_array) else {
        return Ok(mapper);
    };

    for pair in pairs {
        let pair = pair.as_array().ok_or("mapping must be [from, to]")?;

        mapper.add(Mapping {
            input: as_path(pair.first())?,
            output: as_path(pair.get(1))?,
        });
    }

    Ok(mapper)
}

/// A mapping is named by the field it fills, as an index or a path.
fn as_output(params: &Json, name: &str) -> Result<Vec<usize>, String> {
    as_path(Some(&field(params, name)?))
}

/// One side of a mapping: a field index, or a path into nested records.
///
/// `2` and `[2]` mean the same field; `[2, 4]` is field 4 of the record in field 2.
fn as_path(side: Option<&Json>) -> Result<Vec<usize>, String> {
    let path = match side {
        Some(Json::Array(steps)) => steps
            .iter()
            .map(|step| {
                step.as_u64()
                    .map(|step| step as usize)
                    .ok_or_else(|| "mapping path takes field indexes".to_string())
            })
            .collect::<Result<Vec<usize>, String>>()?,

        Some(index) => vec![index.as_u64().ok_or("bad mapping")? as usize],
        None => return Err("mapping must be [from, to]".into()),
    };

    if path.is_empty() {
        return Err("a mapping path needs at least one field".into());
    }

    Ok(path)
}

// ---------- methods ----------

async fn dispatch(app: &mut App, method: &str, params: &Json) -> Result<Json, String> {
    match method {
        "describe" => {
            let mut libraries: Vec<Json> = app
                .libraries()
                .into_iter()
                .map(|(key, descriptor)| library_json(key, descriptor))
                .collect();

            libraries.sort_by(|a, b| a["key"].as_str().cmp(&b["key"].as_str()));

            Ok(json!({ "libraries": libraries }))
        }

        "create_pipeline" => Ok(json!({
            "pipeline": app.create_pipeline(&as_str(params, "name")?)
        })),

        "create_plugin" => {
            let library = as_str(params, "library")?;
            let plugin = app.create_plugin(&library)?;
            let ui = app
                .plugin_ui(&plugin)?
                .map(|path| path.display().to_string());

            Ok(json!({ "plugin": plugin, "library": library, "ui": ui }))
        }

        "remove_plugin" => {
            app.remove_plugin(&as_str(params, "plugin")?).await?;

            Ok(json!({}))
        }

        "add_node" => Ok(json!({
            "node": app.add_node(as_usize(params, "pipeline")?, &as_str(params, "plugin")?)?
        })),

        "connect" => {
            app.connect(
                as_usize(params, "pipeline")?,
                as_usize(params, "from")?,
                as_usize(params, "to")?,
                as_mapper(params, "mappings")?,
            )?;

            Ok(json!({}))
        }

        "disconnect" => Ok(json!({
            "disconnected": app.disconnect(
                as_usize(params, "pipeline")?,
                as_usize(params, "from")?,
                as_usize(params, "to")?,
            )?
        })),

        "remove_mapping" => Ok(json!({
            "removed": app.remove_mapping(
                as_usize(params, "pipeline")?,
                as_usize(params, "from")?,
                as_usize(params, "to")?,
                &as_output(params, "mapping")?,
            )?
        })),

        "remove_subscription_mapping" => Ok(json!({
            "removed": app.remove_subscription_mapping(
                &as_str(params, "from")?,
                as_usize(params, "event")?,
                &as_str(params, "to")?,
                as_usize(params, "command")?,
                &as_output(params, "mapping")?,
            )?
        })),

        "subscribe" => {
            app.subscribe(
                &as_str(params, "from")?,
                as_usize(params, "event")?,
                &as_str(params, "to")?,
                as_usize(params, "command")?,
                as_mapper(params, "mappings")?,
            )?;

            Ok(json!({}))
        }

        "unsubscribe" => {
            app.unsubscribe(
                &as_str(params, "from")?,
                as_usize(params, "event")?,
                &as_str(params, "to")?,
                as_usize(params, "command")?,
            )?;

            Ok(json!({}))
        }

        "set_parameter" => {
            let mut storage = Storage::default();
            let value = storage.value(&field(params, "value")?)?;

            app.set_parameter(
                &as_str(params, "plugin")?,
                as_usize(params, "field")?,
                value,
            )?;

            Ok(json!({}))
        }

        "invoke" => {
            let mut storage = Storage::default();

            let payload = params
                .get("payload")
                .and_then(Json::as_array)
                .cloned()
                .unwrap_or_default();

            let values = payload
                .iter()
                .map(|item| storage.value(item))
                .collect::<Result<Vec<Value>, String>>()?;

            let input = Message {
                values: Slice::from(values.as_slice()),
            };

            app.invoke(
                &as_str(params, "plugin")?,
                as_usize(params, "command")?,
                &input,
            )?;

            Ok(json!({}))
        }

        "set_node_running" => {
            app.set_node_running(
                as_usize(params, "pipeline")?,
                as_usize(params, "node")?,
                field(params, "running")?.as_bool().unwrap_or(false),
            )?;

            Ok(json!({}))
        }

        "set_node_name" => {
            app.set_node_name(
                as_usize(params, "pipeline")?,
                as_usize(params, "node")?,
                &as_str(params, "name")?,
            )?;

            Ok(json!({}))
        }

        "start" => {
            app.start(as_usize(params, "pipeline")?)?;
            Ok(json!({}))
        }

        "stop" => {
            app.stop(as_usize(params, "pipeline")?)?;
            Ok(json!({}))
        }

        _ => Err(format!("unknown method '{method}'")),
    }
}

/// Back to full speed as soon as anything moves, slower while nothing does.
fn backoff(current: Duration, routed: usize) -> Duration {
    match routed {
        0 => (current * 2).min(ROUTE_IDLE_MAX),
        _ => ROUTE_INTERVAL,
    }
}

// ---------- serving ----------

/// One JSON object per line, both directions.
///
/// Requests get `{ id, ok, result | error }` back; published events arrive
/// unsolicited as `{ type: "event", plugin, event, values }`.
pub async fn serve(mut app: App) -> std::io::Result<()> {
    let address = ipc::address();
    let mut listener = ipc::Listener::bind(&address).await?;

    log_info!("listening on {address}");

    loop {
        // Keep routing while nobody is connected: the pipeline runs regardless.
        let mut idle = ROUTE_INTERVAL;

        let stream = loop {
            tokio::select! {
                accepted = listener.accept() => break accepted?,
                _ = tokio::time::sleep(idle) => {
                    idle = backoff(idle, app.route_pending());
                }
            }
        };

        log_info!("client connected");

        serve_client(&mut app, stream).await;

        log_info!("client disconnected");
    }
}

async fn serve_client(app: &mut App, stream: ipc::Stream) {
    let (read, mut write) = tokio::io::split(stream);
    let mut lines = BufReader::new(read).lines();

    let (tap, mut notices) = unbounded_channel::<Notice>();
    event::set_tap(Some(tap));

    let (live_tap, mut moves) = unbounded_channel::<()>();
    live::board().set_tap(Some(live_tap));

    let mut idle = ROUTE_INTERVAL;

    // Due at once: a client that has just connected knows nothing, and every
    // reading on the board is news to it.
    let mut polled = Instant::now()
        .checked_sub(LIVE_INTERVAL)
        .unwrap_or_else(Instant::now);

    // What this client has already been told, so only news is sent.
    let mut refreshed = Instant::now();
    let mut reported: HashMap<String, Vec<Owned>> = HashMap::new();

    loop {
        let messages = tokio::select! {
            line = lines.next_line() => match line {
                Ok(Some(line)) => match respond(app, &line).await {
                    Some(response) => vec![response],
                    None => continue,
                },
                // Client went away, wait for the next one.
                _ => break,
            },

            notice = notices.recv() => match notice {
                Some(notice) => vec![notice_json(&notice)],
                None => continue,
            },

            // A plugin published: out it goes, unless a line just went out.
            // The board stays woken, so the beat below picks it up instead.
            moved = moves.recv() => match moved {
                Some(()) if polled.elapsed() >= LIVE_INTERVAL => {
                    polled = Instant::now();
                    live_news(&mut reported, &mut refreshed)
                }
                _ => continue,
            },

            _ = tokio::time::sleep(idle) => {
                idle = backoff(idle, app.route_pending());

                if polled.elapsed() < LIVE_INTERVAL {
                    continue;
                }

                polled = Instant::now();

                live_news(&mut reported, &mut refreshed)
            }
        };

        let mut line = String::new();

        for message in &messages {
            line.push_str(&message.to_string());
            line.push('\n');
        }

        if line.is_empty() {
            continue;
        }

        if write.write_all(line.as_bytes()).await.is_err() {
            break;
        }
    }

    event::set_tap(None);
    live::board().set_tap(None);

    drain(&mut notices);
    drain(&mut moves);
}

async fn respond(app: &mut App, line: &str) -> Option<Json> {
    if line.trim().is_empty() {
        return None;
    }

    Some(match serde_json::from_str::<Request>(line) {
        Ok(request) => {
            log_debug!("<- #{} {} {}", request.id, request.method, request.params);

            match dispatch(app, &request.method, &request.params).await {
                Ok(result) => json!({ "id": request.id, "ok": true, "result": result }),
                Err(error) => {
                    log_warn!("#{} {} failed: {error}", request.id, request.method);

                    json!({ "id": request.id, "ok": false, "error": error })
                }
            }
        }
        Err(error) => {
            log_warn!("malformed request: {error}");

            json!({ "id": Json::Null, "ok": false, "error": error.to_string() })
        }
    })
}

/// Readings this client has not been told, the whole board on the slow beat:
/// a page that has just opened should not wait for one to move.
fn live_news(reported: &mut HashMap<String, Vec<Owned>>, refreshed: &mut Instant) -> Vec<Json> {
    if refreshed.elapsed() >= LIVE_REFRESH {
        *refreshed = Instant::now();
        reported.clear();
    }

    live_json(live::board().changed(reported))
}

/// One `live` line per plugin whose readings moved.
fn live_json(snapshots: Vec<(String, Vec<Owned>)>) -> Vec<Json> {
    snapshots
        .into_iter()
        .map(|(plugin, values)| {
            json!({
                "type": "live",
                "plugin": plugin,
                "values": values.iter().map(owned_json).collect::<Vec<Json>>(),
            })
        })
        .collect()
}

fn notice_json(notice: &Notice) -> Json {
    log_trace!("-> event {} of '{}'", notice.event, notice.plugin_id);

    json!({
        "type": "event",
        "plugin": notice.plugin_id,
        "event": notice.event,
        "values": notice.values.iter().map(owned_json).collect::<Vec<Json>>(),
    })
}

fn drain<T>(channel: &mut UnboundedReceiver<T>) {
    while channel.try_recv().is_ok() {}
}

#[cfg(test)]
mod tests {
    use mstu_sdk::{Str, TYPE_BYTES, TYPE_F64, TYPE_I32, TYPE_STRING, TYPE_U64};

    use super::*;

    #[test]
    fn a_value_goes_out_as_the_json_of_its_kind() {
        assert_eq!(value_json(&true.into()), json!(true));
        assert_eq!(value_json(&7u64.into()), json!(7));
        assert_eq!(value_json(&(-7i64).into()), json!(-7));
        assert_eq!(value_json(&0.5f64.into()), json!(0.5));
        assert_eq!(value_json(&Str::from_static("h264").into()), json!("h264"));

        // A payload is not sent down the socket.
        assert_eq!(value_json(&Value::none()), Json::Null);
    }

    #[test]
    fn a_reading_goes_out_as_the_json_of_its_kind() {
        assert_eq!(owned_json(&Owned::None), Json::Null);
        assert_eq!(owned_json(&Owned::Uint(7)), json!(7));
        assert_eq!(owned_json(&Owned::Str("a".into())), json!("a"));

        // Bytes travel as how many, since the client cannot use them.
        assert_eq!(owned_json(&Owned::Bytes(64)), json!({ "bytes": 64 }));

        assert_eq!(
            owned_json(&Owned::List(vec![Owned::Uint(1), Owned::Bool(false)])),
            json!([1, false])
        );
    }

    #[test]
    fn a_type_is_named_as_the_desktop_knows_it() {
        assert_eq!(type_name(&TYPE_U64), "uint");
        assert_eq!(type_name(&TYPE_I32), "int");
        assert_eq!(type_name(&TYPE_F64), "float");
        assert_eq!(type_name(&TYPE_STRING), "string");
        assert_eq!(type_name(&TYPE_BYTES), "bytes");
    }

    #[test]
    fn a_missing_field_says_which_one() {
        let params = json!({ "pipeline": 1 });

        assert_eq!(field(&params, "plugin").unwrap_err(), "missing 'plugin'");
        assert_eq!(as_usize(&params, "pipeline").unwrap(), 1);
        assert_eq!(
            as_usize(&json!({ "pipeline": "one" }), "pipeline").unwrap_err(),
            "'pipeline' must be a number"
        );
    }

    #[test]
    fn a_wire_is_read_as_a_pair_of_paths() {
        let params = json!({ "mappings": [[[4], [0]], [[1, 2], [0, 1]]] });

        let mapper = as_mapper(&params, "mappings").expect("a mapper");

        assert!(!mapper.is_empty());
    }

    #[test]
    fn no_mappings_at_all_is_an_empty_mapper_rather_than_an_error() {
        let mapper = as_mapper(&json!({}), "mappings").expect("an empty mapper");

        assert!(mapper.is_empty());
    }

    #[test]
    fn a_wire_that_is_not_a_pair_of_paths_is_refused() {
        for bad in [
            json!({ "mappings": [5] }),
            json!({ "mappings": [[[0]]] }),
            json!({ "mappings": [[["a"], [0]]] }),
        ] {
            assert!(as_mapper(&bad, "mappings").is_err(), "{bad}");
        }
    }

    /// A single index is a path of one, which is how the desktop sends a flat
    /// field.
    #[test]
    fn a_bare_index_is_a_path_of_one_step() {
        assert_eq!(as_path(Some(&json!(3))).unwrap(), vec![3]);
        assert_eq!(as_path(Some(&json!([3, 1]))).unwrap(), vec![3, 1]);
        assert!(as_path(None).is_err());
    }

    #[test]
    fn the_field_a_mapping_fills_is_read_the_same_way() {
        assert_eq!(
            as_output(&json!({ "output": [1, 0] }), "output").unwrap(),
            vec![1, 0]
        );
        assert!(as_output(&json!({}), "output").is_err());
    }

    /// Nothing moving means waiting longer, up to a point; a message moving
    /// means waiting the least again.
    #[test]
    fn the_idle_wait_doubles_while_nothing_is_routed() {
        let mut idle = ROUTE_INTERVAL;

        for _ in 0..10 {
            idle = backoff(idle, 0);
        }

        assert_eq!(idle, ROUTE_IDLE_MAX);
        assert_eq!(backoff(idle, 1), ROUTE_INTERVAL);
    }

    #[test]
    fn a_live_line_names_the_plugin_it_came_from() {
        let lines = live_json(vec![("PLUG-1".to_string(), vec![Owned::Uint(3)])]);

        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0]["type"], "live");
        assert_eq!(lines[0]["plugin"], "PLUG-1");
        assert_eq!(lines[0]["values"], json!([3]));
    }

    #[test]
    fn an_event_line_names_the_event_it_was() {
        let notice = Notice {
            plugin_id: "PLUG-1".to_string(),
            event: 2,
            values: vec![Owned::Str("file.mp4".into())],
        };

        let line = notice_json(&notice);

        assert_eq!(line["type"], "event");
        assert_eq!(line["event"], 2);
        assert_eq!(line["values"], json!(["file.mp4"]));
    }

    /// Everything the desktop draws a node from: its fields, their names and
    /// what it answers to.
    #[test]
    fn a_library_is_described_field_by_field() {
        use crate::runtime::fake::SINK;

        let described = library_json("fake-plugin", &SINK);

        assert_eq!(described["key"], "fake-plugin");
        assert_eq!(described["name"], "fake");
        assert_eq!(
            described["source"], false,
            "it takes an input, so it is no source"
        );

        let input = described["input"]["fields"].as_array().expect("its fields");

        assert_eq!(input.len(), 2);
        assert_eq!(input[0]["name"], "id");
        assert_eq!(input[0]["type"], "uint");
        assert_eq!(input[1]["name"], "name");
        assert_eq!(input[1]["type"], "string");

        // It produces nothing, which is said as null rather than left out.
        assert_eq!(described["output"], Json::Null);

        assert_eq!(described["events"][0]["name"], "happened");
        assert_eq!(described["commands"][0]["name"], "do.it");
    }

    #[test]
    fn a_plugin_that_takes_nothing_is_described_as_a_source() {
        use crate::runtime::fake::SOURCE;

        let described = library_json("fake-source", &SOURCE);

        assert_eq!(described["source"], true);
        assert_eq!(described["input"], Json::Null);
        assert_eq!(described["output"]["fields"][0]["name"], "id");
    }

    #[test]
    fn a_number_is_described_with_its_width_and_sign() {
        let described = type_json(&TYPE_I32, 1);

        assert_eq!(described["type"], "int");
        assert_eq!(described["bits"], 32);
        assert_eq!(described["signed"], true);

        assert_eq!(type_json(&TYPE_U64, 1)["signed"], false);
        assert_eq!(type_json(&TYPE_F64, 1)["bits"], 64);
    }

    #[test]
    fn no_schema_is_described_as_nothing() {
        assert_eq!(schema_json(std::ptr::null()), Json::Null);
    }

    /// What a client sends for set_parameter, on its way across the ABI.
    #[test]
    fn json_becomes_a_value_of_the_nearest_kind() {
        let mut storage = Storage::default();

        assert_eq!(
            storage.value(&json!(null)).unwrap().kind,
            ValueKind::ValueNone
        );
        assert_eq!(
            storage.value(&json!(true)).unwrap().kind,
            ValueKind::ValueBool
        );
        assert_eq!(storage.value(&json!(7)).unwrap().kind, ValueKind::ValueUint);
        assert_eq!(storage.value(&json!(-7)).unwrap().kind, ValueKind::ValueInt);
        assert_eq!(
            storage.value(&json!(0.5)).unwrap().kind,
            ValueKind::ValueFloat
        );
    }

    /// The value borrows the text, so whoever made it has to hold it until
    /// the call is over.
    #[test]
    fn a_string_is_held_for_as_long_as_the_value_points_at_it() {
        let mut storage = Storage::default();

        let value = storage.value(&json!("/tmp/clip.mp4")).unwrap();

        assert_eq!(
            unsafe { value.get::<Str>().unwrap().as_str() },
            "/tmp/clip.mp4"
        );

        // A second string does not move the first.
        let other = storage.value(&json!("another")).unwrap();

        assert_eq!(
            unsafe { value.get::<Str>().unwrap().as_str() },
            "/tmp/clip.mp4"
        );
        assert_eq!(unsafe { other.get::<Str>().unwrap().as_str() }, "another");
    }

    #[test]
    fn what_cannot_cross_the_abi_is_refused_rather_than_flattened() {
        let mut storage = Storage::default();

        assert!(storage.value(&json!([1, 2])).is_err());
        assert!(storage.value(&json!({ "a": 1 })).is_err());
    }

    /// Only what moved, until the slow beat comes round and the whole picture
    /// goes out again.
    #[test]
    fn news_is_only_what_changed_until_the_refresh() {
        let _globals = crate::runtime::fake::globals();

        let plugin = "PLUG-NEWS-TEST";

        live::board().register(
            plugin,
            vec![crate::runtime::schema::Shape {
                kind: ValueKind::ValueUint,
                fields: None,
                element: None,
            }],
        );

        let values = [1u64.into()];
        live::board().publish(
            plugin,
            &Message {
                values: Slice::from_raw_parts(values.as_ptr(), values.len()),
            },
        );

        let mut reported = HashMap::new();
        let mut refreshed = Instant::now();

        let first = live_news(&mut reported, &mut refreshed);
        assert!(first.iter().any(|line| line["plugin"] == plugin));

        // Nothing moved since, so there is no news.
        let second = live_news(&mut reported, &mut refreshed);
        assert!(!second.iter().any(|line| line["plugin"] == plugin));

        // Once the refresh is due, everything is said again.
        let mut due = Instant::now() - LIVE_REFRESH;
        let third = live_news(&mut reported, &mut due);

        assert!(third.iter().any(|line| line["plugin"] == plugin));

        live::board().forget(plugin);
    }
}
