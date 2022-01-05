mod collection;

use std::collections::BTreeMap;

pub use collection::{Collection, Field, Index};
use lookup::{FieldBuf, Lookup, LookupBuf, Segment, SegmentBuf};

/// The type (kind) of a given value.
///
/// This struct tracks the known states a type can have. By allowing one type to have multiple
/// states, the type definition can be progressively refined.
///
/// At the start, a type is in the "any" state, meaning its type can be any of the valid states, as
/// more information becomes available, states can be removed, until one state is left.
///
/// A state without any type information (e.g. all fields are `None`) is an invalid invariant, and
/// is checked against by the API exposed by this type.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Kind {
    bytes: Option<()>,
    integer: Option<()>,
    float: Option<()>,
    boolean: Option<()>,
    timestamp: Option<()>,
    regex: Option<()>,
    null: Option<()>,
    array: Option<Collection<collection::Index>>,
    object: Option<Collection<collection::Field>>,
}

impl Kind {
    /// Get the inner object collection.
    ///
    /// This returns `None` if the type is not known to be an object.
    #[must_use]
    pub fn as_object(&self) -> Option<&Collection<collection::Field>> {
        self.object.as_ref()
    }

    /// Get a mutable reference to the inner object collection.
    ///
    /// This returns `None` if the type is not known to be an object.
    #[must_use]
    pub fn as_object_mut(&mut self) -> Option<&mut Collection<collection::Field>> {
        self.object.as_mut()
    }

    /// Take an object `Collection` type out of the `Kind`.
    ///
    /// This returns `None` if the type is not known to be an object.
    #[must_use]
    pub fn into_object(self) -> Option<Collection<collection::Field>> {
        self.object
    }

    /// Get the inner array collection.
    ///
    /// This returns `None` if the type is not known to be an array.
    #[must_use]
    pub fn as_array(&self) -> Option<&Collection<collection::Index>> {
        self.array.as_ref()
    }

    /// Get a mutable reference to the inner array collection.
    ///
    /// This returns `None` if the type is not known to be an array.
    #[must_use]
    pub fn as_array_mut(&mut self) -> Option<&mut Collection<collection::Index>> {
        self.array.as_mut()
    }

    /// Take an array `Collection` type out of the `Kind`.
    ///
    /// This returns `None` if the type is not known to be an array.
    #[must_use]
    pub fn into_array(self) -> Option<Collection<collection::Index>> {
        self.array
    }

    /// Returns `Kind`, with non-primitive states removed.
    ///
    /// That is, it returns `self,` but removes the `object` and `array` states.
    ///
    /// Returns `None` if no primitive states are set.
    #[must_use]
    pub fn to_primitive(mut self) -> Option<Self> {
        self.remove_array().ok()?;
        self.remove_object().ok()?;

        Some(self)
    }

    /// Check if other is contained within self.
    ///
    /// If `Kind` is a non-collection type, it needs to match exactly, but if it has a collection
    /// type, then the known fields in `self` need to be present in `other`, but not the other way
    /// around.
    #[must_use]
    pub fn contains(&self, other: &Self) -> bool {
        let mut matched = false;

        match (self.bytes, other.bytes) {
            (None, Some(_)) => return false,
            (Some(_), Some(_)) => matched = true,
            _ => {}
        };

        match (self.integer, other.integer) {
            (None, Some(_)) => return false,
            (Some(_), Some(_)) => matched = true,
            _ => {}
        };

        match (self.float, other.float) {
            (None, Some(_)) => return false,
            (Some(_), Some(_)) => matched = true,
            _ => {}
        };

        match (self.boolean, other.boolean) {
            (None, Some(_)) => return false,
            (Some(_), Some(_)) => matched = true,
            _ => {}
        };

        match (self.timestamp, other.timestamp) {
            (None, Some(_)) => return false,
            (Some(_), Some(_)) => matched = true,
            _ => {}
        };

        match (self.regex, other.regex) {
            (None, Some(_)) => return false,
            (Some(_), Some(_)) => matched = true,
            _ => {}
        };

        match (self.null, other.null) {
            (None, Some(_)) => return false,
            (Some(_), Some(_)) => matched = true,
            _ => {}
        };

        match (self.array.as_ref(), other.array.as_ref()) {
            (None, Some(_)) => return false,
            (Some(this), Some(other)) if this.contains(other) => matched = true,
            _ => {}
        };

        match (self.object.as_ref(), other.object.as_ref()) {
            (None, Some(_)) => return false,
            (Some(this), Some(other)) if this.contains(other) => matched = true,
            _ => {}
        };

        matched
    }

    /// Merge `other` type into `self`.
    ///
    /// The provided [`MergeStrategy`] determines how the merge happens.
    pub fn merge(&mut self, other: Self, strategy: MergeStrategy) {
        // Merge primitive types by setting their state to "present" if needed.
        self.bytes = self.bytes.or(other.bytes);
        self.integer = self.integer.or(other.integer);
        self.float = self.float.or(other.float);
        self.boolean = self.boolean.or(other.boolean);
        self.timestamp = self.timestamp.or(other.timestamp);
        self.regex = self.regex.or(other.regex);
        self.null = self.null.or(other.null);

        // Merge collection types by picking one if the other is none, or merging both..
        self.array = match (self.array.take(), other.array) {
            (None, None) => None,
            (this @ Some(..), None) => this,
            (None, other @ Some(..)) => other,
            (Some(..), other @ Some(..)) if strategy.collections.is_shallow() => other,
            (Some(mut this), Some(other)) => {
                this.merge(other, false);
                Some(this)
            }
        };
        self.object = match (self.object.take(), other.object) {
            (None, None) => None,
            (this @ Some(..), None) => this,
            (None, other @ Some(..)) => other,
            (Some(..), other @ Some(..)) if strategy.collections.is_shallow() => other,
            (Some(mut this), Some(other)) => {
                this.merge(other, false);
                Some(this)
            }
        };
    }

    /// Similar to `merge`, except that non-collection types are overwritten instead of merged.
    pub fn merge_collections(&mut self, other: Self) {
        self.bytes = other.bytes;
        self.integer = other.integer;
        self.float = other.float;
        self.boolean = other.boolean;
        self.timestamp = other.timestamp;
        self.regex = other.regex;
        self.null = other.null;

        self.array = match (self.array.take(), other.array) {
            (None | Some(..), None) => None,
            (None, other @ Some(..)) => other,
            (Some(mut this), Some(other)) => {
                this.merge(other, true);
                Some(this)
            }
        };
        self.object = match (self.object.take(), other.object) {
            (None | Some(..), None) => None,
            (None, other @ Some(..)) => other,
            (Some(mut this), Some(other)) => {
                this.merge(other, true);
                Some(this)
            }
        };
    }

    /// Nest the given [`Kind`] into a provided path.
    ///
    /// For example, given an `integer` kind and a path `.foo`, a new `Kind` is returned that is
    /// known to be an object, of which the `foo` field is known to be an `integer`.
    #[must_use]
    pub fn nest_at_path(mut self, path: &Lookup<'_>) -> Self {
        for segment in path.iter().rev() {
            match segment {
                Segment::Field(lookup::Field { name, .. }) => {
                    let field = (*name).to_owned();
                    let map = BTreeMap::from([(field.into(), self)]);

                    self = Self::object(map);
                }
                Segment::Coalesce(fields) => {
                    // FIXME(Jean): this is incorrect, since we need to handle all fields. This bug
                    // already existed in the existing `vrl::TypeDef` implementation, and since
                    // we're doing alway with path coalescing, let's not bother with fixing this
                    // for now.
                    let field = fields.last().expect("at least one").name.to_owned();
                    let map = BTreeMap::from([(field.into(), self)]);

                    self = Self::object(map);
                }
                Segment::Index(index) => {
                    // For negative indices, we have to mark the array contents as unknown, since
                    // we can't be sure of the size of the array.
                    let collection = if index.is_negative() {
                        Collection::any()
                    } else {
                        #[allow(clippy::cast_sign_loss)]
                        let index = *index as usize;
                        let map = BTreeMap::from([(index.into(), self)]);
                        Collection::from(map)
                    };

                    self = Self::array(collection);
                }
            }
        }

        self
    }

    /// Find the `Kind` at the given path.
    ///
    /// if the path points to an unknown location, `None` is returned.
    #[must_use]
    pub fn find_at_path(&self, path: LookupBuf) -> Option<Self> {
        let mut iter = path.into_iter();

        let kind = match iter.next() {
            // We've reached the end of the path segments, so return whatever kind we're currently
            // into.
            None => return Some(self.clone()),

            // We have one or more segments to parse, do so and optionally recursively call this
            // function for the relevant kind.
            Some(segment) => match segment {
                SegmentBuf::Coalesce(fields) => match self.as_object() {
                    // We got a coalesced field, but we don't have an object to fetch the path
                    // from, so there's no `kind` to find at the given path.
                    None => return None,

                    // We have an object, but know nothing of its fields, so all we can say is that
                    // there _might_ be a field at the given path, but it could be of any type.
                    Some(collection) if collection.is_any() => return Some(Kind::any()),

                    // We have an object with one or more known fields. Try to find the requested
                    // field within the collection, or use the default "other" field type info.
                    Some(collection) => fields
                        .into_iter()
                        .find_map(|field| collection.known().get(&field.into()).cloned())
                        .unwrap_or_else(|| collection.other()),
                },

                SegmentBuf::Field(FieldBuf { name: field, .. }) => match self.as_object() {
                    // We got a field, but we don't have an object to fetch the path from, so
                    // there's no `kind` to find at the given path.
                    None => return None,

                    // We have an object, but know nothing of its fields, so all we can say is that
                    // there _might_ be a field at the given path, but it could be of any type.
                    Some(collection) if collection.is_any() => return Some(Kind::any()),

                    // We have an object with one or more known fields. Try to find the requested
                    // field within the collection, or use the default "other" field type info.
                    Some(collection) => collection
                        .known()
                        .get(&field.into())
                        .cloned()
                        .unwrap_or_else(|| collection.other()),
                },

                SegmentBuf::Index(index) => match self.as_array() {
                    // We got an index, but we don't have an array to index into, so there's no
                    // `kind` to find at the given path.
                    None => return None,

                    // If we're trying to get a negative index, we have to return "any", since we
                    // never have a full picture of the shape of an array, so we can't index from
                    // the end of the array.
                    Some(_) if index.is_negative() => return Some(Kind::any()),

                    #[allow(clippy::cast_sign_loss)]
                    Some(collection) => collection
                        .known()
                        .get(&(index as usize).into())
                        .cloned()
                        .unwrap_or_else(|| collection.other()),
                },
            },
        };

        kind.find_at_path(LookupBuf::from_segments(iter.collect()))
    }

    /// Returns `true` if there is a known kind at the given path.
    #[must_use]
    pub fn has_path(&self, path: LookupBuf) -> bool {
        self.find_at_path(path).is_some()
    }
}

// Initializer functions.
impl Kind {
    /// The "any" type state.
    ///
    /// This state implies all states for the type are valid. There is no known information that
    /// can be gleaned from the type.
    #[must_use]
    pub fn any() -> Self {
        Self {
            bytes: Some(()),
            integer: Some(()),
            float: Some(()),
            boolean: Some(()),
            timestamp: Some(()),
            regex: Some(()),
            null: Some(()),
            array: Some(Collection::any()),
            object: Some(Collection::any()),
        }
    }

    /// The "json" type state.
    ///
    /// This state is similar to `any`, except that it excludes any types that can't be represented
    /// in a native JSON-type (such as `timestamp` and `regex`).
    #[must_use]
    pub fn json() -> Self {
        Self {
            bytes: Some(()),
            integer: Some(()),
            float: Some(()),
            boolean: Some(()),
            timestamp: None,
            regex: None,
            null: Some(()),
            array: Some(Collection::json()),
            object: Some(Collection::json()),
        }
    }

    /// The "primitive" type state.
    ///
    /// This state represents all types, _except_ ones that contain collection of types (e.g.
    /// objects and arrays).
    #[must_use]
    pub fn primitive() -> Self {
        Self {
            bytes: Some(()),
            integer: Some(()),
            float: Some(()),
            boolean: Some(()),
            timestamp: Some(()),
            regex: Some(()),
            null: Some(()),
            array: None,
            object: None,
        }
    }

    /// The "bytes" type state.
    #[must_use]
    pub fn bytes() -> Self {
        Self {
            bytes: Some(()),
            integer: None,
            float: None,
            boolean: None,
            timestamp: None,
            regex: None,
            null: None,
            array: None,
            object: None,
        }
    }

    /// The "integer" type state.
    #[must_use]
    pub fn integer() -> Self {
        Self {
            bytes: None,
            integer: Some(()),
            float: None,
            boolean: None,
            timestamp: None,
            regex: None,
            null: None,
            array: None,
            object: None,
        }
    }

    /// The "float" type state.
    #[must_use]
    pub fn float() -> Self {
        Self {
            bytes: None,
            integer: None,
            float: Some(()),
            boolean: None,
            timestamp: None,
            regex: None,
            null: None,
            array: None,
            object: None,
        }
    }

    /// The "boolean" type state.
    #[must_use]
    pub fn boolean() -> Self {
        Self {
            bytes: None,
            integer: None,
            float: None,
            boolean: Some(()),
            timestamp: None,
            regex: None,
            null: None,
            array: None,
            object: None,
        }
    }

    /// The "timestamp" type state.
    #[must_use]
    pub fn timestamp() -> Self {
        Self {
            bytes: None,
            integer: None,
            float: None,
            boolean: None,
            timestamp: Some(()),
            regex: None,
            null: None,
            array: None,
            object: None,
        }
    }

    /// The "regex" type state.
    #[must_use]
    pub fn regex() -> Self {
        Self {
            bytes: None,
            integer: None,
            float: None,
            boolean: None,
            timestamp: None,
            regex: Some(()),
            null: None,
            array: None,
            object: None,
        }
    }

    /// The "null" type state.
    #[must_use]
    pub fn null() -> Self {
        Self {
            bytes: None,
            integer: None,
            float: None,
            boolean: None,
            timestamp: None,
            regex: None,
            null: Some(()),
            array: None,
            object: None,
        }
    }

    /// The "array" type state.
    #[must_use]
    pub fn array(map: impl Into<Collection<collection::Index>>) -> Self {
        Self {
            bytes: None,
            integer: None,
            float: None,
            boolean: None,
            timestamp: None,
            regex: None,
            null: None,
            array: Some(map.into()),
            object: None,
        }
    }

    /// The "object" type state.
    #[must_use]
    pub fn object(map: impl Into<Collection<collection::Field>>) -> Self {
        Self {
            bytes: None,
            integer: None,
            float: None,
            boolean: None,
            timestamp: None,
            regex: None,
            null: None,
            array: None,
            object: Some(map.into()),
        }
    }

    /// The "empty" state of a type.
    ///
    /// NOTE: We do NOT want to expose this state publicly, as its an invalid invariant to have
    ///       a type state with all variants set to "none".
    #[allow(unused)]
    fn empty() -> Self {
        Self {
            bytes: None,
            integer: None,
            float: None,
            boolean: None,
            timestamp: None,
            regex: None,
            null: None,
            array: None,
            object: None,
        }
    }

    /// Check for the "empty" state of a type.
    ///
    /// NOTE: We do NOT want to expose this method publicly, as its an invalid invariant to have
    ///       a type state with all variants set to "none".
    #[allow(unused)]
    fn is_empty(&self) -> bool {
        !self.is_bytes()
            && !self.is_integer()
            && !self.is_float()
            && !self.is_boolean()
            && !self.is_timestamp()
            && !self.is_regex()
            && !self.is_null()
            && !self.is_array()
            && !self.is_object()
    }
}

// `or_*` methods to extend the state of a type using a builder-like API.
impl Kind {
    /// Add the `bytes` state to the type.
    #[must_use]
    pub fn or_bytes(mut self) -> Self {
        self.bytes = Some(());
        self
    }

    /// Add the `integer` state to the type.
    #[must_use]
    pub fn or_integer(mut self) -> Self {
        self.integer = Some(());
        self
    }

    /// Add the `float` state to the type.
    #[must_use]
    pub fn or_float(mut self) -> Self {
        self.float = Some(());
        self
    }

    /// Add the `boolean` state to the type.
    #[must_use]
    pub fn or_boolean(mut self) -> Self {
        self.boolean = Some(());
        self
    }

    /// Add the `timestamp` state to the type.
    #[must_use]
    pub fn or_timestamp(mut self) -> Self {
        self.timestamp = Some(());
        self
    }

    /// Add the `regex` state to the type.
    #[must_use]
    pub fn or_regex(mut self) -> Self {
        self.regex = Some(());
        self
    }

    /// Add the `null` state to the type.
    #[must_use]
    pub fn or_null(mut self) -> Self {
        self.null = Some(());
        self
    }

    /// Add the `array` state to the type.
    #[must_use]
    pub fn or_array(mut self, map: BTreeMap<collection::Index, Kind>) -> Self {
        self.array = Some(map.into());
        self
    }

    /// Add the `object` state to the type.
    #[must_use]
    pub fn or_object(mut self, map: BTreeMap<collection::Field, Kind>) -> Self {
        self.object = Some(map.into());
        self
    }
}

// `is_*` functions to check the state of a type.
impl Kind {
    /// Returns `true` if all type states are valid.
    ///
    /// That is, this method only returns `true` if the object matches _all_ of the known types.
    #[must_use]
    pub fn is_any(&self) -> bool {
        self.is_bytes()
            && self.is_integer()
            && self.is_float()
            && self.is_boolean()
            && self.is_timestamp()
            && self.is_regex()
            && self.is_null()
            && self.is_array()
            && self.is_object()
    }

    /// Returns `true` if only primitive type states are valid.
    #[must_use]
    pub fn is_primitive(&self) -> bool {
        !self.is_empty() && !self.is_object() && !self.is_array()
    }

    /// Returns `true` if the type is _at least_ `bytes`.
    ///
    /// Note that other type states can also still be valid, for exact matching, also compare
    /// against `is_exact()`.
    #[must_use]
    pub fn is_bytes(&self) -> bool {
        self.bytes.is_some()
    }

    /// Returns `true` if the type is _at least_ `integer`.
    ///
    /// Note that other type states can also still be valid, for exact matching, also compare
    /// against `is_exact()`.
    #[must_use]
    pub fn is_integer(&self) -> bool {
        self.integer.is_some()
    }

    /// Returns `true` if the type is _at least_ `float`.
    ///
    /// Note that other type states can also still be valid, for exact matching, also compare
    /// against `is_exact()`.
    #[must_use]
    pub fn is_float(&self) -> bool {
        self.float.is_some()
    }

    /// Returns `true` if the type is _at least_ `boolean`.
    ///
    /// Note that other type states can also still be valid, for exact matching, also compare
    /// against `is_exact()`.
    #[must_use]
    pub fn is_boolean(&self) -> bool {
        self.boolean.is_some()
    }

    /// Returns `true` if the type is _at least_ `timestamp`.
    ///
    /// Note that other type states can also still be valid, for exact matching, also compare
    /// against `is_exact()`.
    #[must_use]
    pub fn is_timestamp(&self) -> bool {
        self.timestamp.is_some()
    }

    /// Returns `true` if the type is _at least_ `regex`.
    ///
    /// Note that other type states can also still be valid, for exact matching, also compare
    /// against `is_exact()`.
    #[must_use]
    pub fn is_regex(&self) -> bool {
        self.regex.is_some()
    }

    /// Returns `true` if the type is _at least_ `null`.
    ///
    /// Note that other type states can also still be valid, for exact matching, also compare
    /// against `is_exact()`.
    #[must_use]
    pub fn is_null(&self) -> bool {
        self.null.is_some()
    }

    /// Returns `true` if the type is _at least_ `array`.
    ///
    /// Note that other type states can also still be valid, for exact matching, also compare
    /// against `is_exact()`.
    #[must_use]
    pub fn is_array(&self) -> bool {
        self.array.is_some()
    }

    /// Returns `true` if the type is _at least_ `object`.
    ///
    /// Note that other type states can also still be valid, for exact matching, also compare
    /// against `is_exact()`.
    #[must_use]
    pub fn is_object(&self) -> bool {
        self.object.is_some()
    }

    /// Returns `true` if exactly one type is set.
    ///
    /// For example, the following:
    ///
    /// ```rust,ignore
    /// kind.is_float() && kind.is_exact()
    /// ```
    ///
    /// Returns `true` only if the type is exactly a float.
    #[must_use]
    #[allow(clippy::many_single_char_names)]
    pub fn is_exact(&self) -> bool {
        let a = self.is_bytes();
        let b = self.is_integer();
        if a && b {
            return false;
        }

        let c = self.is_float();
        if !(!c || !a && !b) {
            return false;
        }

        let d = self.is_boolean();
        if !(!d || !a && !b && !c) {
            return false;
        }

        let e = self.is_timestamp();
        if !(!e || !a && !b && !c && !d) {
            return false;
        }

        let f = self.is_regex();
        if !(!f || !a && !b && !c && !d && !e) {
            return false;
        }

        let g = self.is_null();
        if !(!g || !a && !b && !c && !d && !e && !f) {
            return false;
        }

        let h = self.is_array();
        if !(!h || !a && !b && !c && !d && !e && !f && !g) {
            return false;
        }

        let i = self.is_object();
        if !(!i || !a && !b && !c && !d && !e && !f && !g) {
            return false;
        }

        true
    }
}

// `add_*` methods to extend the state of a type.
impl Kind {
    /// Add the `bytes` state to the type.
    ///
    /// If the type already included this state, the function returns `false`.
    pub fn add_bytes(&mut self) -> bool {
        self.bytes.replace(()).is_none()
    }

    /// Add the `integer` state to the type.
    ///
    /// If the type already included this state, the function returns `false`.
    pub fn add_integer(&mut self) -> bool {
        self.integer.replace(()).is_none()
    }

    /// Add the `float` state to the type.
    ///
    /// If the type already included this state, the function returns `false`.
    pub fn add_float(&mut self) -> bool {
        self.float.replace(()).is_none()
    }

    /// Add the `boolean` state to the type.
    ///
    /// If the type already included this state, the function returns `false`.
    pub fn add_boolean(&mut self) -> bool {
        self.boolean.replace(()).is_none()
    }

    /// Add the `timestamp` state to the type.
    ///
    /// If the type already included this state, the function returns `false`.
    pub fn add_timestamp(&mut self) -> bool {
        self.timestamp.replace(()).is_none()
    }

    /// Add the `regex` state to the type.
    ///
    /// If the type already included this state, the function returns `false`.
    pub fn add_regex(&mut self) -> bool {
        self.regex.replace(()).is_none()
    }

    /// Add the `null` state to the type.
    ///
    /// If the type already included this state, the function returns `false`.
    pub fn add_null(&mut self) -> bool {
        self.null.replace(()).is_none()
    }

    /// Add the `array` state to the type.
    ///
    /// If the type already included this state, the function returns `false`.
    pub fn add_array(&mut self, map: BTreeMap<collection::Index, Kind>) -> bool {
        self.array.replace(map.into()).is_none()
    }

    /// Add the `object` state to the type.
    ///
    /// If the type already included this state, the function returns `false`.
    pub fn add_object(&mut self, map: BTreeMap<collection::Field, Kind>) -> bool {
        self.object.replace(map.into()).is_none()
    }
}

// `remove_*` methods to narrow the state of a type.
impl Kind {
    /// Remove the `bytes` state from the type.
    ///
    /// If the type already excluded this state, the function returns `Ok(false)`.
    ///
    /// # Errors
    ///
    /// If removing this state leaves an "empty" type, then the error variant is returned. This was
    /// chosen, because when applying progressive type checking, there should _always_ be at least
    /// one state for a given type, the "no state left for a type" variant is a programming error.
    pub fn remove_bytes(&mut self) -> Result<bool, EmptyKindError> {
        if self.is_bytes() && self.is_exact() {
            return Err(EmptyKindError);
        }

        Ok(self.bytes.take().is_none())
    }

    /// Remove the `integer` state from the type.
    ///
    /// If the type already excluded this state, the function returns `Ok(false)`.
    ///
    /// # Errors
    ///
    /// If removing this state leaves an "empty" type, then the error variant is returned. This was
    /// chosen, because when applying progressive type checking, there should _always_ be at least
    /// one state for a given type, the "no state left for a type" variant is a programming error.
    pub fn remove_integer(&mut self) -> Result<bool, EmptyKindError> {
        if self.is_integer() && self.is_exact() {
            return Err(EmptyKindError);
        }

        Ok(self.integer.take().is_none())
    }

    /// Remove the `float` state from the type.
    ///
    /// If the type already excluded this state, the function returns `Ok(false)`.
    ///
    /// # Errors
    ///
    /// If removing this state leaves an "empty" type, then the error variant is returned. This was
    /// chosen, because when applying progressive type checking, there should _always_ be at least
    /// one state for a given type, the "no state left for a type" variant is a programming error.
    pub fn remove_float(&mut self) -> Result<bool, EmptyKindError> {
        if self.is_float() && self.is_exact() {
            return Err(EmptyKindError);
        }

        Ok(self.float.take().is_none())
    }

    /// Remove the `boolean` state from the type.
    ///
    /// If the type already excluded this state, the function returns `Ok(false)`.
    ///
    /// # Errors
    ///
    /// If removing this state leaves an "empty" type, then the error variant is returned. This was
    /// chosen, because when applying progressive type checking, there should _always_ be at least
    /// one state for a given type, the "no state left for a type" variant is a programming error.
    pub fn remove_boolean(&mut self) -> Result<bool, EmptyKindError> {
        if self.is_boolean() && self.is_exact() {
            return Err(EmptyKindError);
        }

        Ok(self.boolean.take().is_none())
    }

    /// Remove the `timestamp` state from the type.
    ///
    /// If the type already excluded this state, the function returns `Ok(false)`.
    ///
    /// # Errors
    ///
    /// If removing this state leaves an "empty" type, then the error variant is returned. This was
    /// chosen, because when applying progressive type checking, there should _always_ be at least
    /// one state for a given type, the "no state left for a type" variant is a programming error.
    pub fn remove_timestamp(&mut self) -> Result<bool, EmptyKindError> {
        if self.is_timestamp() && self.is_exact() {
            return Err(EmptyKindError);
        }

        Ok(self.timestamp.take().is_none())
    }

    /// Remove the `regex` state from the type.
    ///
    /// If the type already excluded this state, the function returns `Ok(false)`.
    ///
    /// # Errors
    ///
    /// If removing this state leaves an "empty" type, then the error variant is returned. This was
    /// chosen, because when applying progressive type checking, there should _always_ be at least
    /// one state for a given type, the "no state left for a type" variant is a programming error.
    pub fn remove_regex(&mut self) -> Result<bool, EmptyKindError> {
        if self.is_regex() && self.is_exact() {
            return Err(EmptyKindError);
        }

        Ok(self.regex.take().is_none())
    }

    /// Remove the `null` state from the type.
    ///
    /// If the type already excluded this state, the function returns `Ok(false)`.
    ///
    /// # Errors
    ///
    /// If removing this state leaves an "empty" type, then the error variant is returned. This was
    /// chosen, because when applying progressive type checking, there should _always_ be at least
    /// one state for a given type, the "no state left for a type" variant is a programming error.
    pub fn remove_null(&mut self) -> Result<bool, EmptyKindError> {
        if self.is_null() && self.is_exact() {
            return Err(EmptyKindError);
        }

        Ok(self.null.take().is_none())
    }

    /// Remove the `array` state from the type.
    ///
    /// If the type already excluded this state, the function returns `Ok(false)`.
    ///
    /// # Errors
    ///
    /// If removing this state leaves an "empty" type, then the error variant is returned. This was
    /// chosen, because when applying progressive type checking, there should _always_ be at least
    /// one state for a given type, the "no state left for a type" variant is a programming error.
    pub fn remove_array(&mut self) -> Result<bool, EmptyKindError> {
        if self.is_array() && self.is_exact() {
            return Err(EmptyKindError);
        }

        Ok(self.array.take().is_none())
    }

    /// Remove the `object` state from the type.
    ///
    /// If the type already excluded this state, the function returns `Ok(false)`.
    ///
    /// # Errors
    ///
    /// If removing this state leaves an "empty" type, then the error variant is returned. This was
    /// chosen, because when applying progressive type checking, there should _always_ be at least
    /// one state for a given type, the "no state left for a type" variant is a programming error.
    pub fn remove_object(&mut self) -> Result<bool, EmptyKindError> {
        if self.is_object() && self.is_exact() {
            return Err(EmptyKindError);
        }

        Ok(self.object.take().is_none())
    }
}

impl From<Collection<Field>> for Kind {
    fn from(collection: Collection<Field>) -> Self {
        Kind::object(collection)
    }
}

impl From<Collection<Index>> for Kind {
    fn from(collection: Collection<Index>) -> Self {
        Kind::array(collection)
    }
}

impl std::fmt::Display for Kind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_any() {
            return f.write_str("any");
        }

        let mut kinds = vec![];

        if self.is_bytes() {
            kinds.push("string");
        }
        if self.is_integer() {
            kinds.push("integer");
        }
        if self.is_float() {
            kinds.push("float");
        }
        if self.is_boolean() {
            kinds.push("boolean");
        }
        if self.is_timestamp() {
            kinds.push("timestamp");
        }
        if self.is_regex() {
            kinds.push("regex");
        }
        if self.is_null() {
            kinds.push("null");
        }
        if self.is_array() {
            kinds.push("array");
        }
        if self.is_object() {
            kinds.push("object");
        }

        let last = kinds.remove(0);

        if kinds.is_empty() {
            return last.fmt(f);
        }

        let mut kinds = kinds.into_iter().peekable();

        while let Some(kind) = kinds.next() {
            kind.fmt(f)?;

            if kinds.peek().is_some() {
                f.write_str(", ")?;
            }
        }

        f.write_str(" or ")?;
        last.fmt(f)?;

        Ok(())
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum KnownFieldMergeStrategy {
    /// Merge the type of known fields.
    ///
    /// That is, given:
    ///
    /// ```json,ignore
    /// { "foo": true }
    /// ```
    ///
    /// merging with:
    ///
    /// ```json,ignore
    /// { "foo": 12 }
    /// ```
    ///
    /// The type for known field `foo` becomes [boolean, integer]. Meaning the field will either be
    /// a boolean or an integer at runtime.
    Merge,

    /// Overwrite the type of a known field.
    /// That is, given:
    ///
    /// ```json,ignore
    /// { "foo": true }
    /// ```
    ///
    /// merging with:
    ///
    /// ```json,ignore
    /// { "foo": 12 }
    /// ```
    ///
    /// The type for known field `foo` becomes [integer]. Meaning the field will be an integer, and
    /// can no longer be a boolean.
    Overwrite,
}

impl Default for KnownFieldMergeStrategy {
    fn default() -> Self {
        Self::Merge
    }
}

/// The strategy used to merge two [`Kind`]s
#[derive(Default, Debug, Copy, Clone, PartialEq)]
pub struct MergeStrategy {
    /// The merge strategy for known fields.
    pub known_fields: KnownFieldMergeStrategy,

    /// The merge strategy for collections.
    pub collections: collection::MergeStrategy,
}

#[derive(Debug)]
pub struct EmptyKindError;

impl std::fmt::Display for EmptyKindError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("invalid empty type state variant")
    }
}

impl std::error::Error for EmptyKindError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nest_at_path() {
        // object
        let mut kind1 = Kind::integer();
        kind1 = kind1.nest_at_path(&LookupBuf::from_str(".foo.bar").unwrap().to_lookup());

        let map1 = BTreeMap::from([("bar".into(), Kind::integer())]);
        let map2 = BTreeMap::from([("foo".into(), Kind::object(map1))]);
        let valid1 = Kind::object(map2);

        assert_eq!(kind1, valid1);

        // array
        let mut kind2 = Kind::boolean();
        kind2 = kind2.nest_at_path(&LookupBuf::from_str(".foo[2]").unwrap().to_lookup());

        let map1 = BTreeMap::from([(2.into(), Kind::boolean())]);
        let map2 = BTreeMap::from([("foo".into(), Kind::array(map1))]);
        let valid2 = Kind::object(map2);

        assert_eq!(kind2, valid2);
    }

    #[test]
    fn test_merge() {
        // (any) & (any) = (any)
        let mut left = Kind::any();
        let right = Kind::any();
        let want = Kind::any();
        left.merge(right, MergeStrategy::default());
        assert_eq!(left, want);

        // (any) & (null) = (any)
        let mut left = Kind::any();
        let right = Kind::null();
        left.merge(right, MergeStrategy::default());
        assert_eq!(left, want);

        // (null) & (null) = (null)
        let mut left = Kind::null();
        let right = Kind::null();
        left.merge(right, MergeStrategy::default());
        assert_eq!(left, Kind::null());

        // (null, timestamp) & (bytes, boolean) = (null, timestamp, bytes, boolean)
        let mut left = Kind::null();
        left.add_timestamp();
        let mut right = Kind::bytes();
        right.add_boolean();
        left.merge(right, MergeStrategy::default());
        assert_eq!(left, {
            let mut want = Kind::null();
            want.add_timestamp();
            want.add_boolean();
            want.add_bytes();
            want
        });
    }

    #[test]
    fn kind_is_exact() {
        let kind = Kind::any();
        assert!(!kind.is_exact());

        let kind = Kind::json();
        assert!(!kind.is_exact());

        let kind = Kind::boolean().or_float();
        assert!(!kind.is_exact());

        let kind = Kind::timestamp();
        assert!(kind.is_exact());

        let kind = Kind::array(BTreeMap::default());
        assert!(kind.is_exact());
    }

    #[test]
    fn test_contains() {
        // TODO
    }
}