use mstu_sdk::{Message, Schema, Type, TypeKind, Value, ValueKind};

/// The value kind a field of this type must carry.
fn kind_of(ty: &Type) -> ValueKind {
    match ty.kind {
        TypeKind::TypeBool => ValueKind::ValueBool,
        TypeKind::TypeInt => {
            if unsafe { ty.schema.int_ }.signed {
                ValueKind::ValueInt
            } else {
                ValueKind::ValueUint
            }
        }
        TypeKind::TypeFloat => ValueKind::ValueFloat,
        // Enum variants travel as their string value.
        TypeKind::TypeString | TypeKind::TypeEnum => ValueKind::ValueString,
        TypeKind::TypeBytes => ValueKind::ValueBytes,
        TypeKind::TypeList => ValueKind::ValueList,
        TypeKind::TypeRecord => ValueKind::ValueRecord,
    }
}

/// How deep a schema is described before it is taken on trust, matching the
/// depth the desktop is told about.
const MAX_DEPTH: usize = 8;

/// The shape one field must have, down to what a record or a list holds.
///
/// Owned, rather than read from the plugin's schema on the spot, because it
/// is kept beside a running node and used on every message.
#[derive(Clone)]
pub struct Shape {
    pub kind: ValueKind,

    /// A record's fields. None below `MAX_DEPTH`, where anything is accepted.
    pub fields: Option<Vec<Shape>>,

    /// What a list holds, None for the same reason.
    pub element: Option<Box<Shape>>,
}

/// The shape a message must have to satisfy `schema`. A null schema means
/// "no message", so nothing is expected.
pub fn shape_of(schema: *const Schema) -> Vec<Shape> {
    if schema.is_null() {
        return Vec::new();
    }

    fields_shape(unsafe { &*schema }, 1)
}

fn fields_shape(schema: &Schema, depth: usize) -> Vec<Shape> {
    unsafe { schema.fields.as_slice() }
        .iter()
        .map(|field| shape_of_type(field.ty, depth))
        .collect()
}

fn shape_of_type(ty: &Type, depth: usize) -> Shape {
    let mut shape = Shape {
        kind: kind_of(ty),
        fields: None,
        element: None,
    };

    if depth >= MAX_DEPTH {
        return shape;
    }

    match ty.kind {
        TypeKind::TypeRecord => {
            let record = unsafe { ty.schema.record_ };
            shape.fields = Some(fields_shape(record.schema, depth + 1));
        }

        TypeKind::TypeList => {
            let list = unsafe { ty.schema.list_ };
            shape.element = Some(Box::new(shape_of_type(list.element, depth + 1)));
        }

        _ => {}
    }

    shape
}

/// Retypes a number to the kind its field declares.
///
/// JSON has one number type, so `0` arrives as a uint even where the schema
/// asks for a float, and a plugin reading it by the declared type sees nothing.
pub fn coerce(value: Value, expected: ValueKind) -> Value {
    if value.kind == expected {
        return value;
    }

    unsafe {
        match (expected, value.kind) {
            (ValueKind::ValueFloat, ValueKind::ValueUint) => (value.data.uint_ as f64).into(),
            (ValueKind::ValueFloat, ValueKind::ValueInt) => (value.data.int_ as f64).into(),
            (ValueKind::ValueUint, ValueKind::ValueInt) if value.data.int_ >= 0 => {
                (value.data.int_ as u64).into()
            }
            (ValueKind::ValueInt, ValueKind::ValueUint) => (value.data.uint_ as i64).into(),
            _ => value,
        }
    }
}

/// The kind a single field of `schema` demands.
pub fn field_kind(schema: *const Schema, field: usize) -> Option<ValueKind> {
    if schema.is_null() {
        return None;
    }

    unsafe { (*schema).fields.as_slice() }
        .get(field)
        .map(|field| kind_of(field.ty))
}

/// Plugins read their input by schema and unwrap, so a message that does not
/// match is a crash waiting to happen. The engine never delivers one.
pub fn matches(message: &Message, expected: &[Shape]) -> bool {
    let values = message.values_slice();

    values.len() == expected.len() && values.iter().zip(expected).all(|(value, shape)| fits(value, shape))
}

/// A record is checked field by field, and a list element by element: a
/// wrongly shaped record is as fatal to a plugin as a wrongly typed field.
fn fits(value: &Value, shape: &Shape) -> bool {
    if value.kind != shape.kind {
        return false;
    }

    match value.kind {
        ValueKind::ValueRecord => match &shape.fields {
            Some(fields) => {
                let held = unsafe { value.data.record_.as_slice() };

                held.len() == fields.len()
                    && held.iter().zip(fields).all(|(value, shape)| fits(value, shape))
            }
            None => true,
        },

        ValueKind::ValueList => match &shape.element {
            Some(element) => unsafe { value.data.list_.as_slice() }
                .iter()
                .all(|value| fits(value, element)),
            None => true,
        },

        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use mstu_sdk::{Slice, Str, schema};

    use super::*;

    fn plain(kind: ValueKind) -> Shape {
        Shape {
            kind,
            fields: None,
            element: None,
        }
    }

    fn message(values: &[Value]) -> Message {
        Message {
            values: Slice::from_raw_parts(values.as_ptr(), values.len()),
        }
    }

    static FIELDS: [schema::Field; 3] = [
        schema::Field {
            name: Str::from_static("when"),
            ty: &schema::TYPE_U64,
            description: Str::from_static(""),
        },
        schema::Field {
            name: Str::from_static("sync"),
            ty: &schema::TYPE_BOOL,
            description: Str::from_static(""),
        },
        schema::Field {
            name: Str::from_static("name"),
            ty: &schema::TYPE_STRING,
            description: Str::from_static(""),
        },
    ];

    static SCHEMA: Schema = Schema {
        fields: Slice::from_raw_parts(FIELDS.as_ptr(), FIELDS.len()),
    };

    #[test]
    fn a_schema_becomes_one_shape_per_field() {
        let shapes = shape_of(&SCHEMA);

        assert_eq!(shapes.len(), 3);
        assert_eq!(shapes[0].kind, ValueKind::ValueUint);
        assert_eq!(shapes[1].kind, ValueKind::ValueBool);
        assert_eq!(shapes[2].kind, ValueKind::ValueString);
    }

    #[test]
    fn no_schema_expects_no_message() {
        assert!(shape_of(std::ptr::null()).is_empty());
    }

    #[test]
    fn a_message_matches_the_shape_it_was_built_for() {
        let values = [1u64.into(), true.into(), Str::from_static("a").into()];

        assert!(matches(&message(&values), &shape_of(&SCHEMA)));
    }

    #[test]
    fn a_field_of_the_wrong_kind_does_not_match() {
        let values = [1u64.into(), 2u64.into(), Str::from_static("a").into()];

        assert!(!matches(&message(&values), &shape_of(&SCHEMA)));
    }

    #[test]
    fn a_message_of_the_wrong_length_does_not_match() {
        let values = [1u64.into(), true.into()];

        assert!(!matches(&message(&values), &shape_of(&SCHEMA)));
    }

    #[test]
    fn a_record_is_checked_field_by_field() {
        let inner = [1u64.into(), Str::from_static("a").into()];
        let record = Value::record(Slice::from_raw_parts(inner.as_ptr(), inner.len()));

        let expected = [Shape {
            kind: ValueKind::ValueRecord,
            fields: Some(vec![
                plain(ValueKind::ValueUint),
                plain(ValueKind::ValueString),
            ]),
            element: None,
        }];

        assert!(matches(&message(&[record]), &expected));

        // The same values, the wrong way round.
        let swapped = [Str::from_static("a").into(), 1u64.into()];
        let record = Value::record(Slice::from_raw_parts(swapped.as_ptr(), swapped.len()));

        assert!(!matches(&message(&[record]), &expected));
    }

    #[test]
    fn a_number_is_given_the_kind_its_field_asks_for() {
        // A client's JSON carries no type, so 1 arrives as whichever the
        // parser felt like.
        assert_eq!(
            coerce(1i64.into(), ValueKind::ValueUint).kind,
            ValueKind::ValueUint
        );
        assert_eq!(
            coerce(1u64.into(), ValueKind::ValueFloat).kind,
            ValueKind::ValueFloat
        );

        // Nothing sensible to do, so it is left as it is.
        assert_eq!(
            coerce(Str::from_static("a").into(), ValueKind::ValueUint).kind,
            ValueKind::ValueString
        );

        // A negative number is not a count.
        assert_eq!(
            coerce((-1i64).into(), ValueKind::ValueUint).kind,
            ValueKind::ValueInt
        );
    }

    #[test]
    fn a_field_names_the_kind_it_wants() {
        assert_eq!(field_kind(&SCHEMA, 1), Some(ValueKind::ValueBool));
        assert_eq!(field_kind(&SCHEMA, 9), None);
        assert_eq!(field_kind(std::ptr::null(), 0), None);
    }

    /// Below the depth the desktop is told about, anything is taken on trust,
    /// since a shape that was never described cannot be checked.
    #[test]
    fn a_record_nothing_describes_is_taken_on_trust() {
        let anything = Shape {
            kind: ValueKind::ValueRecord,
            fields: None,
            element: None,
        };

        let inner = [1u64.into(), true.into()];
        let record = Value::record(Slice::from_raw_parts(inner.as_ptr(), inner.len()));

        assert!(matches(&message(&[record]), &[anything]));
    }

    #[test]
    fn a_list_is_checked_by_what_it_holds() {
        let of_numbers = Shape {
            kind: ValueKind::ValueList,
            fields: None,
            element: Some(Box::new(plain(ValueKind::ValueUint))),
        };

        let numbers = [1u64.into(), 2u64.into()];
        let list = Value::list(Slice::from_raw_parts(numbers.as_ptr(), numbers.len()));

        assert!(matches(&message(&[list]), &[of_numbers.clone()]));

        let mixed = [1u64.into(), Str::from_static("two").into()];
        let list = Value::list(Slice::from_raw_parts(mixed.as_ptr(), mixed.len()));

        assert!(!matches(&message(&[list]), &[of_numbers]));
    }

    #[test]
    fn an_empty_list_fits_whatever_it_was_meant_to_hold() {
        let of_numbers = Shape {
            kind: ValueKind::ValueList,
            fields: None,
            element: Some(Box::new(plain(ValueKind::ValueUint))),
        };

        assert!(matches(&message(&[Value::list(Slice::empty())]), &[of_numbers]));
    }

    #[test]
    fn a_record_of_the_wrong_width_does_not_fit() {
        let expected = [Shape {
            kind: ValueKind::ValueRecord,
            fields: Some(vec![plain(ValueKind::ValueUint), plain(ValueKind::ValueBool)]),
            element: None,
        }];

        let short = [1u64.into()];
        let record = Value::record(Slice::from_raw_parts(short.as_ptr(), short.len()));

        assert!(!matches(&message(&[record]), &expected));
    }

    /// Enum variants travel as the string they are named by.
    #[test]
    fn an_enum_is_carried_as_text() {
        use mstu_sdk::{EnumSchema, EnumVariant, TypeSchema};

        static VARIANTS: [EnumVariant; 1] = [EnumVariant {
            name: Str::from_static("h264"),
            value: Value::none(),
        }];

        static CODEC: EnumSchema = EnumSchema {
            variants: Slice::from_raw_parts(VARIANTS.as_ptr(), VARIANTS.len()),
        };

        static CODEC_TYPE: schema::Type = schema::Type {
            kind: schema::TypeKind::TypeEnum,
            schema: TypeSchema { enum_: &CODEC },
        };

        static ENUM_FIELDS: [schema::Field; 1] = [schema::Field {
            name: Str::from_static("codec"),
            ty: &CODEC_TYPE,
            description: Str::empty(),
        }];

        static ENUM_SCHEMA: Schema = Schema {
            fields: Slice::from_raw_parts(ENUM_FIELDS.as_ptr(), ENUM_FIELDS.len()),
        };

        let shapes = shape_of(&ENUM_SCHEMA);

        assert_eq!(shapes[0].kind, ValueKind::ValueString);
        assert!(matches(&message(&[Str::from_static("h264").into()]), &shapes));
    }
}
