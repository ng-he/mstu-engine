use mstu_sdk::{Slice, Str, Value, ValueKind};

use crate::runtime::alloc::Arena;

/// Deep-copies a value into `arena`.
///
/// Everything a message carries by reference — strings, bytes, lists and
/// records — is copied, because the value outlives whoever handed it over:
/// the plugin call that wrote it, or the message a mapping read it from.
///
/// # Safety
///
/// `value` must be a valid value whose buffers are still alive.
pub unsafe fn clone_value(arena: &mut Arena, value: &Value) -> Value {
    unsafe {
        match value.kind {
            ValueKind::ValueNone => Value::none(),

            ValueKind::ValueBool
            | ValueKind::ValueInt
            | ValueKind::ValueUint
            | ValueKind::ValueFloat => *value,

            ValueKind::ValueString => clone_str(arena, value.data.string_).into(),
            ValueKind::ValueBytes => clone_slice(arena, value.data.bytes_).into(),
            ValueKind::ValueList => Value::list(clone_values(arena, value.data.list_)),

            // Not `.into()`: that would hand back a list, since both carry the
            // same slice.
            ValueKind::ValueRecord => Value::record(clone_values(arena, value.data.record_)),
        }
    }
}

/// Takes a value a plugin wrote, copying only what the arena does not already
/// hold: a payload written into reserved memory is taken where it lies.
///
/// # Safety
///
/// `value` must be a valid value whose buffers are still alive.
pub unsafe fn take_value(arena: &mut Arena, value: &Value) -> Value {
    unsafe {
        match value.kind {
            ValueKind::ValueBytes if arena.owns(value.data.bytes_.ptr) => *value,
            ValueKind::ValueString if arena.owns(value.data.string_.ptr) => *value,
            _ => clone_value(arena, value),
        }
    }
}

/// # Safety
///
/// `slice` must be valid for `slice.len` elements, or empty.
pub unsafe fn clone_slice<T: Copy>(arena: &mut Arena, slice: Slice<T>) -> Slice<T> {
    if slice.ptr.is_null() || slice.len == 0 {
        return Slice::empty();
    }

    let dst = arena.alloc::<T>(slice.len);

    unsafe {
        std::ptr::copy_nonoverlapping(slice.ptr, dst, slice.len);
    }

    Slice::from_raw_parts(dst, slice.len)
}

/// # Safety
///
/// `slice` must be valid for `slice.len` values, or empty.
pub unsafe fn clone_values(arena: &mut Arena, slice: Slice<Value>) -> Slice<Value> {
    if slice.ptr.is_null() || slice.len == 0 {
        return Slice::empty();
    }

    let src = unsafe { slice.as_slice() };
    let dst = arena.alloc::<Value>(src.len());

    for (index, value) in src.iter().enumerate() {
        unsafe { dst.add(index).write(clone_value(arena, value)) };
    }

    Slice::from_raw_parts(dst, src.len())
}

/// # Safety
///
/// `value` must point at `value.len` bytes of UTF-8, or be empty.
pub unsafe fn clone_str(arena: &mut Arena, value: Str) -> Str {
    if value.ptr.is_null() || value.len == 0 {
        return Str::empty();
    }

    let dst = arena.alloc::<u8>(value.len);

    unsafe {
        std::ptr::copy_nonoverlapping(value.ptr, dst, value.len);
    }

    Str {
        ptr: dst,
        len: value.len,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arena() -> Arena {
        Arena::new(4096)
    }

    #[test]
    fn a_number_is_carried_as_it_is() {
        let mut arena = arena();

        for value in [
            Value::none(),
            true.into(),
            7u64.into(),
            (-7i64).into(),
            0.5f64.into(),
        ] {
            let copy = unsafe { clone_value(&mut arena, &value) };

            assert_eq!(copy.kind, value.kind);
        }

        // Nothing by reference, so nothing was allocated.
        assert_eq!(arena.chunk_count(), 0);
    }

    #[test]
    fn a_string_is_copied_away_from_whoever_wrote_it() {
        let mut arena = arena();

        let text = String::from("dump.h264");
        let value: Value = Str::new(text.as_str()).into();

        let copy = unsafe { clone_value(&mut arena, &value) };

        assert!(arena.owns(unsafe { copy.data.string_ }.ptr));
        assert_ne!(unsafe { copy.data.string_ }.ptr, unsafe { value.data.string_ }.ptr);

        drop(text);

        assert_eq!(unsafe { copy.get::<Str>().unwrap().as_str() }, "dump.h264");
    }

    #[test]
    fn bytes_are_copied_with_their_contents() {
        let mut arena = arena();

        let payload = vec![1u8, 2, 3, 4];
        let value: Value = Slice::from_raw_parts(payload.as_ptr(), payload.len()).into();

        let copy = unsafe { clone_value(&mut arena, &value) };
        let bytes = unsafe { copy.data.bytes_ };

        assert!(arena.owns(bytes.ptr));
        assert_eq!(unsafe { bytes.as_slice() }, &[1, 2, 3, 4]);
    }

    /// A record cloned as a list arrives with the wrong shape, which is how
    /// this was found the first time.
    #[test]
    fn a_record_stays_a_record_and_a_list_a_list() {
        let mut arena = arena();

        let inner = [1u64.into(), Str::from_static("a").into()];
        let slice = Slice::from_raw_parts(inner.as_ptr(), inner.len());

        let record = unsafe { clone_value(&mut arena, &Value::record(slice)) };
        let list = unsafe { clone_value(&mut arena, &Value::list(slice)) };

        assert_eq!(record.kind, ValueKind::ValueRecord);
        assert_eq!(list.kind, ValueKind::ValueList);
    }

    #[test]
    fn what_a_list_holds_is_copied_too() {
        let mut arena = arena();

        let text = String::from("inside");
        let inner = [Value::from(Str::new(text.as_str())), 2u64.into()];
        let slice = Slice::from_raw_parts(inner.as_ptr(), inner.len());

        let copy = unsafe { clone_value(&mut arena, &Value::list(slice)) };
        let values = unsafe { copy.data.list_.as_slice() };

        drop(text);

        assert_eq!(values.len(), 2);
        assert_eq!(unsafe { values[0].get::<Str>().unwrap().as_str() }, "inside");
        assert_eq!(values[1].get::<u64>(), Some(2));
    }

    #[test]
    fn an_empty_string_or_slice_needs_no_memory() {
        let mut arena = arena();

        assert_eq!(unsafe { clone_str(&mut arena, Str::empty()) }.len, 0);
        assert_eq!(unsafe { clone_slice::<u8>(&mut arena, Slice::empty()) }.len, 0);
        assert_eq!(unsafe { clone_values(&mut arena, Slice::empty()) }.len, 0);

        assert_eq!(arena.chunk_count(), 0);
    }

    /// The whole point of reserving: a payload written into the arena is taken
    /// where it lies.
    #[test]
    fn what_the_arena_already_holds_is_not_copied_again() {
        let mut arena = arena();

        let reserved = arena.alloc::<u8>(16);
        let value: Value = Slice::from_raw_parts(reserved as *const u8, 16).into();

        let taken = unsafe { take_value(&mut arena, &value) };

        assert_eq!(unsafe { taken.data.bytes_ }.ptr, reserved as *const u8);
    }

    #[test]
    fn what_the_plugin_still_owns_is_copied() {
        let mut arena = arena();

        let payload = vec![9u8; 16];
        let value: Value = Slice::from_raw_parts(payload.as_ptr(), payload.len()).into();

        let taken = unsafe { take_value(&mut arena, &value) };
        let bytes = unsafe { taken.data.bytes_ };

        assert_ne!(bytes.ptr, payload.as_ptr());
        assert!(arena.owns(bytes.ptr));
        assert_eq!(unsafe { bytes.as_slice() }, &payload[..]);
    }

    #[test]
    fn a_string_the_arena_holds_is_taken_as_it_is_too() {
        let mut arena = arena();

        let reserved = arena.alloc::<u8>(4);
        unsafe { std::ptr::copy_nonoverlapping(b"abcd".as_ptr(), reserved, 4) };

        let value: Value = Str {
            ptr: reserved as *const u8,
            len: 4,
        }
        .into();

        let taken = unsafe { take_value(&mut arena, &value) };

        assert_eq!(unsafe { taken.data.string_ }.ptr, reserved as *const u8);
    }
}
