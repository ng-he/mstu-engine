use mstu_sdk::{Message, Value, ValueKind};

/// A message value copied out of the ABI so it can outlive the plugin call.
#[derive(Debug, Clone, PartialEq)]
pub enum Owned {
    None,
    Bool(bool),
    Int(i64),
    Uint(u64),
    Float(f64),
    Str(String),
    Bytes(usize),
    List(Vec<Owned>),
}

pub fn own(value: &Value) -> Owned {
    unsafe {
        match value.kind {
            ValueKind::ValueNone => Owned::None,
            ValueKind::ValueBool => Owned::Bool(value.data.bool_),
            ValueKind::ValueInt => Owned::Int(value.data.int_),
            ValueKind::ValueUint => Owned::Uint(value.data.uint_),
            ValueKind::ValueFloat => Owned::Float(value.data.float_),
            ValueKind::ValueString => Owned::Str(value.data.string_.as_str().to_string()),

            // Payloads are for display: keep the size, not the bytes.
            ValueKind::ValueBytes => Owned::Bytes(value.data.bytes_.len),

            ValueKind::ValueList | ValueKind::ValueRecord => {
                Owned::List(value.data.list_.as_slice().iter().map(own).collect())
            }
        }
    }
}

pub fn own_message(message: &Message) -> Vec<Owned> {
    message.values_slice().iter().map(own).collect()
}

#[cfg(test)]
mod tests {
    use mstu_sdk::{Slice, Str};

    use super::*;

    #[test]
    fn every_kind_comes_out_as_itself() {
        assert_eq!(own(&Value::none()), Owned::None);
        assert_eq!(own(&true.into()), Owned::Bool(true));
        assert_eq!(own(&7u64.into()), Owned::Uint(7));
        assert_eq!(own(&(-7i64).into()), Owned::Int(-7));
        assert_eq!(own(&0.5f64.into()), Owned::Float(0.5));
    }

    #[test]
    fn a_string_is_copied_rather_than_pointed_at() {
        let text = String::from("dump.h264");
        let value: Value = Str::new(text.as_str()).into();

        let owned = own(&value);
        drop(text);

        assert_eq!(owned, Owned::Str("dump.h264".to_string()));
    }

    /// A payload is for display, so what is kept is how big it was.
    #[test]
    fn a_payload_keeps_its_size_and_not_its_bytes() {
        let bytes = [0u8; 64];
        let value: Value = Slice::from_raw_parts(bytes.as_ptr(), bytes.len()).into();

        assert_eq!(own(&value), Owned::Bytes(64));
    }

    #[test]
    fn a_record_and_a_list_are_both_kept_in_order() {
        let inner = [1u64.into(), Str::from_static("a").into()];
        let slice = Slice::from_raw_parts(inner.as_ptr(), inner.len());

        let expected = Owned::List(vec![Owned::Uint(1), Owned::Str("a".to_string())]);

        assert_eq!(own(&Value::list(slice)), expected);
        assert_eq!(own(&Value::record(slice)), expected);
    }

    #[test]
    fn a_message_comes_out_field_by_field() {
        let values = [7u64.into(), true.into()];
        let message = Message {
            values: Slice::from_raw_parts(values.as_ptr(), values.len()),
        };

        assert_eq!(
            own_message(&message),
            vec![Owned::Uint(7), Owned::Bool(true)]
        );
    }

    #[test]
    fn an_empty_message_owns_nothing() {
        let message = Message {
            values: Slice::empty(),
        };

        assert!(own_message(&message).is_empty());
    }
}
