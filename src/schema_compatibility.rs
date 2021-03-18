//! Logic for checking schema compatibility
use crate::schema::{SchemaKind, SchemaType};
use std::{
    collections::{hash_map::DefaultHasher, HashSet},
    hash::Hasher,
    ptr,
};

pub struct SchemaCompatibility;

struct Checker {
    recursion: HashSet<(u64, u64)>,
}

impl Checker {
    /// Create a new checker, with recursion set to an empty set.
    pub(crate) fn new() -> Self {
        Self {
            recursion: HashSet::new(),
        }
    }

    pub(crate) fn can_read(
        &mut self,
        writers_schema: &SchemaType,
        readers_schema: &SchemaType,
    ) -> bool {
        self.full_match_schemas(writers_schema, readers_schema)
    }

    pub(crate) fn full_match_schemas(
        &mut self,
        writers_schema: &SchemaType,
        readers_schema: &SchemaType,
    ) -> bool {
        if self.recursion_in_progress(writers_schema, readers_schema) {
            return true;
        }

        if !SchemaCompatibility::match_schemas(writers_schema, readers_schema) {
            return false;
        }

        let w_type = SchemaKind::from(writers_schema);
        let r_type = SchemaKind::from(readers_schema);

        if w_type != SchemaKind::Union && (r_type.is_primitive() || r_type == SchemaKind::Fixed) {
            return true;
        }

        match r_type {
            SchemaKind::Record => self.match_record_schemas(writers_schema, readers_schema),
            SchemaKind::Map => {
                if let SchemaType::Map(w_m) = writers_schema {
                    if let SchemaType::Map(r_m) = readers_schema {
                        self.full_match_schemas(&w_m.items(), &r_m.items())
                    } else {
                        unreachable!("readers_schema should have been SchemaType::Map")
                    }
                } else {
                    unreachable!("writers_schema should have been SchemaType::Map")
                }
            }
            SchemaKind::Array => {
                if let SchemaType::Array(w_a) = writers_schema {
                    if let SchemaType::Array(r_a) = readers_schema {
                        self.full_match_schemas(&w_a.items(), &r_a.items())
                    } else {
                        unreachable!("readers_schema should have been SchemaType::Array")
                    }
                } else {
                    unreachable!("writers_schema should have been SchemaType::Array")
                }
            }
            SchemaKind::Union => self.match_union_schemas(writers_schema, readers_schema),
            SchemaKind::Enum => {
                // reader's symbols must contain all writer's symbols
                if let SchemaType::Enum(w_e) = writers_schema {
                    if let SchemaType::Enum(r_e) = readers_schema {
                        return w_e
                            .symbols()
                            .iter()
                            .find(|e| !r_e.symbols().contains(e))
                            .is_none();
                    }
                }
                false
            }
            _ => {
                if w_type == SchemaKind::Union {
                    if let SchemaType::Union(r) = writers_schema {
                        if r.variants().len() == 1 {
                            return self.full_match_schemas(&r.variants()[0], readers_schema);
                        } else if r.variants().len() == 0 {
                            // There is a test for this. Does is make sense?
                            return true;
                        }
                    }
                }
                false
            }
        }
    }

    fn match_record_schemas(
        &mut self,
        writers_schema: &SchemaType,
        readers_schema: &SchemaType,
    ) -> bool {
        let w_type = SchemaKind::from(writers_schema);

        if w_type == SchemaKind::Union {
            return false;
        }

        if let SchemaType::Record(w_record) = writers_schema {
            if let SchemaType::Record(r_record) = readers_schema {
                for field in r_record.iter_fields() {
                    if let Some(pos) = w_record.field(field.name()) {
                        if !self.full_match_schemas(&pos.schema(), &field.schema()) {
                            return false;
                        }
                    } else if field.default().is_none() {
                        return false;
                    }
                }
            }
        }
        true
    }

    fn match_union_schemas(
        &mut self,
        writers_schema: &SchemaType,
        readers_schema: &SchemaType,
    ) -> bool {
        let w_type = SchemaKind::from(writers_schema);
        let r_type = SchemaKind::from(readers_schema);

        assert_eq!(r_type, SchemaKind::Union);

        if w_type == SchemaKind::Union {
            if let SchemaType::Union(u) = writers_schema {
                u.variants()
                    .iter()
                    .all(|schema| self.full_match_schemas(schema, readers_schema))
            } else {
                unreachable!("writers_schema should have been SchemaType::Union")
            }
        } else if let SchemaType::Union(u) = readers_schema {
            u.variants()
                .iter()
                .any(|schema| self.full_match_schemas(writers_schema, schema))
        } else {
            unreachable!("readers_schema should have been SchemaType::Union")
        }
    }

    fn recursion_in_progress(
        &mut self,
        writers_schema: &SchemaType,
        readers_schema: &SchemaType,
    ) -> bool {
        let mut hasher = DefaultHasher::new();
        ptr::hash(writers_schema, &mut hasher);
        let w_hash = hasher.finish();

        hasher = DefaultHasher::new();
        ptr::hash(readers_schema, &mut hasher);
        let r_hash = hasher.finish();

        let key = (w_hash, r_hash);
        // This is a shortcut to add if not exists *and* return false. It will return true
        // if it was able to insert.
        !self.recursion.insert(key)
    }
}

impl SchemaCompatibility {
    /// `can_read` performs a full, recursive check that a datum written using the
    /// writers_schema can be read using the readers_schema.
    pub fn can_read(writers_schema: &SchemaType, readers_schema: &SchemaType) -> bool {
        let mut c = Checker::new();
        c.can_read(writers_schema, readers_schema)
    }

    /// `mutual_read` performs a full, recursive check that a datum written using either
    /// the writers_schema or the readers_schema can be read using the other schema.
    pub fn mutual_read(writers_schema: &SchemaType, readers_schema: &SchemaType) -> bool {
        SchemaCompatibility::can_read(writers_schema, readers_schema)
            && SchemaCompatibility::can_read(readers_schema, writers_schema)
    }

    ///  `match_schemas` performs a basic check that a datum written with the
    ///  writers_schema could be read using the readers_schema. This check only includes
    ///  matching the types, including schema promotion, and matching the full name for
    ///  named types. Aliases for named types are not supported here, and the rust
    ///  implementation of Avro in general does not include support for aliases (I think).
    pub(crate) fn match_schemas(writers_schema: &SchemaType, readers_schema: &SchemaType) -> bool {
        let w_type = SchemaKind::from(writers_schema);
        let r_type = SchemaKind::from(readers_schema);

        if w_type == SchemaKind::Union || r_type == SchemaKind::Union {
            return true;
        }

        if w_type == r_type {
            if r_type.is_primitive() {
                return true;
            }

            match r_type {
                SchemaKind::Record => {
                    if let SchemaType::Record(w_record) = writers_schema {
                        if let SchemaType::Record(r_record) = readers_schema {
                            return w_record.name() == r_record.name();
                        } else {
                            unreachable!("readers_schema should have been SchemaType::Record")
                        }
                    } else {
                        unreachable!("writers_schema should have been SchemaType::Record")
                    }
                }
                SchemaKind::Fixed => {
                    if let SchemaType::Fixed(w_fixed) = writers_schema {
                        if let SchemaType::Fixed(r_fixed) = readers_schema {
                            return w_fixed.name() == r_fixed.name()
                                && w_fixed.size() == r_fixed.size();
                        } else {
                            unreachable!("readers_schema should have been SchemaType::Fixed")
                        }
                    } else {
                        unreachable!("writers_schema should have been SchemaType::Fixed")
                    }
                }
                SchemaKind::Enum => {
                    if let SchemaType::Enum(w_e) = writers_schema {
                        if let SchemaType::Enum(r_e) = readers_schema {
                            return w_e.name() == r_e.name();
                        } else {
                            unreachable!("readers_schema should have been SchemaType::Enum")
                        }
                    } else {
                        unreachable!("writers_schema should have been SchemaType::Enum")
                    }
                }
                SchemaKind::Map => {
                    if let SchemaType::Map(w_m) = writers_schema {
                        if let SchemaType::Map(r_m) = readers_schema {
                            return SchemaCompatibility::match_schemas(&w_m.items(), &r_m.items());
                        } else {
                            unreachable!("readers_schema should have been SchemaType::Map")
                        }
                    } else {
                        unreachable!("writers_schema should have been SchemaType::Map")
                    }
                }
                SchemaKind::Array => {
                    if let SchemaType::Array(w_a) = writers_schema {
                        if let SchemaType::Array(r_a) = readers_schema {
                            return SchemaCompatibility::match_schemas(&w_a.items(), &r_a.items());
                        } else {
                            unreachable!("readers_schema should have been SchemaType::Array")
                        }
                    } else {
                        unreachable!("writers_schema should have been SchemaType::Array")
                    }
                }
                _ => (),
            };
        }

        if w_type == SchemaKind::Int
            && vec![SchemaKind::Long, SchemaKind::Float, SchemaKind::Double]
                .iter()
                .any(|&t| t == r_type)
        {
            return true;
        }

        if w_type == SchemaKind::Long
            && vec![SchemaKind::Float, SchemaKind::Double]
                .iter()
                .any(|&t| t == r_type)
        {
            return true;
        }

        if w_type == SchemaKind::Float && r_type == SchemaKind::Double {
            return true;
        }

        if w_type == SchemaKind::String && r_type == SchemaKind::Bytes {
            return true;
        }

        if w_type == SchemaKind::Bytes && r_type == SchemaKind::String {
            return true;
        }

        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::builder::SchemaBuilder;
    use crate::schema::Schema;

    fn int_array_schema() -> Schema {
        Schema::parse_str(r#"{"type":"array", "items":"int"}"#).unwrap()
    }

    fn long_array_schema() -> Schema {
        Schema::parse_str(r#"{"type":"array", "items":"long"}"#).unwrap()
    }

    fn string_array_schema() -> Schema {
        Schema::parse_str(r#"{"type":"array", "items":"string"}"#).unwrap()
    }

    fn int_map_schema() -> Schema {
        Schema::parse_str(r#"{"type":"map", "values":"int"}"#).unwrap()
    }

    fn long_map_schema() -> Schema {
        Schema::parse_str(r#"{"type":"map", "values":"long"}"#).unwrap()
    }

    fn string_map_schema() -> Schema {
        Schema::parse_str(r#"{"type":"map", "values":"string"}"#).unwrap()
    }

    fn enum1_ab_schema() -> Schema {
        Schema::parse_str(r#"{"type":"enum", "name":"Enum1", "symbols":["A","B"]}"#).unwrap()
    }

    fn enum1_abc_schema() -> Schema {
        Schema::parse_str(r#"{"type":"enum", "name":"Enum1", "symbols":["A","B","C"]}"#).unwrap()
    }

    fn enum1_bc_schema() -> Schema {
        Schema::parse_str(r#"{"type":"enum", "name":"Enum1", "symbols":["B","C"]}"#).unwrap()
    }

    fn enum2_ab_schema() -> Schema {
        Schema::parse_str(r#"{"type":"enum", "name":"Enum2", "symbols":["A","B"]}"#).unwrap()
    }

    fn empty_record1_schema() -> Schema {
        Schema::parse_str(r#"{"type":"record", "name":"Record1", "fields":[]}"#).unwrap()
    }

    fn empty_record2_schema() -> Schema {
        Schema::parse_str(r#"{"type":"record", "name":"Record2", "fields": []}"#).unwrap()
    }

    fn a_int_record1_schema() -> Schema {
        Schema::parse_str(
            r#"{"type":"record", "name":"Record1", "fields":[{"name":"a", "type":"int"}]}"#,
        )
        .unwrap()
    }

    fn a_long_record1_schema() -> Schema {
        Schema::parse_str(
            r#"{"type":"record", "name":"Record1", "fields":[{"name":"a", "type":"long"}]}"#,
        )
        .unwrap()
    }

    fn a_int_b_int_record1_schema() -> Schema {
        Schema::parse_str(r#"{"type":"record", "name":"Record1", "fields":[{"name":"a", "type":"int"}, {"name":"b", "type":"int"}]}"#).unwrap()
    }

    fn a_dint_record1_schema() -> Schema {
        Schema::parse_str(r#"{"type":"record", "name":"Record1", "fields":[{"name":"a", "type":"int", "default":0}]}"#).unwrap()
    }

    fn a_int_b_dint_record1_schema() -> Schema {
        Schema::parse_str(r#"{"type":"record", "name":"Record1", "fields":[{"name":"a", "type":"int"}, {"name":"b", "type":"int", "default":0}]}"#).unwrap()
    }

    fn a_dint_b_dint_record1_schema() -> Schema {
        Schema::parse_str(r#"{"type":"record", "name":"Record1", "fields":[{"name":"a", "type":"int", "default":0}, {"name":"b", "type":"int", "default":0}]}"#).unwrap()
    }

    fn nested_record() -> Schema {
        Schema::parse_str(r#"{"type":"record","name":"parent","fields":[{"name":"attribute","type":{"type":"record","name":"child","fields":[{"name":"id","type":"string"}]}}]}"#).unwrap()
    }

    fn nested_optional_record() -> Schema {
        Schema::parse_str(r#"{"type":"record","name":"parent","fields":[{"name":"attribute","type":["null",{"type":"record","name":"child","fields":[{"name":"id","type":"string"}]}],"default":null}]}"#).unwrap()
    }

    fn int_list_record_schema() -> Schema {
        Schema::parse_str(r#"{"type":"record", "name":"List", "fields": [{"name": "head", "type": "int"},{"name": "tail", "type": { "type":"array", "items": "int"} }]}"#).unwrap()
    }

    fn long_list_record_schema() -> Schema {
        Schema::parse_str(
            r#"
      {
        "type":"record", "name":"List", "fields": [
          {"name": "head", "type": "long"},
          {"name": "tail", "type": {"type": "array", "items": "long"}}
      ]}
"#,
        )
        .unwrap()
    }

    fn union_schema(schemas: Vec<Schema>) -> Schema {
        let schema_string = schemas
            .iter()
            .map(|s| s.canonical_form())
            .collect::<Vec<String>>()
            .join(",");
        Schema::parse_str(&format!("[{}]", schema_string)).unwrap()
    }

    fn empty_union_schema() -> Schema {
        union_schema(vec![])
    }

    // unused
    // fn null_union_schema() -> Schema { union_schema(vec![Schema::Null]) }

    fn int_union_schema() -> Schema {
        // union_schema(
        //     // vec![Schema::Int]
        //     vec![Schema::parse(&Value::Array(vec![Value::from(1_i32)]))
        //         .expect("failed to init int union schema")],
        // )
        let builder = SchemaBuilder::new();
        let root = builder.int();

        builder
            .build(root)
            .expect("failed to build int union schema")
    }

    fn long_union_schema() -> Schema {
        // union_schema(vec![Schema::Long])
        let builder = SchemaBuilder::new();
        let root = builder.long();

        builder
            .build(root)
            .expect("failed to build long union schema")
    }

    fn string_union_schema() -> Schema {
        // union_schema(vec![Schema::String])
        let mut builder = SchemaBuilder::new();
        let mut union_builder = builder.union();
        union_builder.variant(builder.string());
        let root = union_builder
            .build(&mut builder)
            .expect("failed to build union schema");

        builder
            .build(root)
            .expect("failed to build string union schema")
    }

    fn int_string_union_schema() -> Schema {
        // union_schema(vec![Schema::Int, Schema::String])
        let mut builder = SchemaBuilder::new();
        let string_schema = builder.string();
        let int_schema = builder.int();
        let mut union_builder = builder.union();
        union_builder.variant(int_schema);
        union_builder.variant(string_schema);
        let union = union_builder
            .build(&mut builder)
            .expect("failed to build int string union");

        builder
            .build(union)
            .expect("failed to build int string union schema")
    }

    fn string_int_union_schema() -> Schema {
        // union_schema(vec![Schema::String, Schema::Int])
        let mut builder = SchemaBuilder::new();
        let string_schema = builder.string();
        let int_schema = builder.int();
        let mut union_builder = builder.union();
        union_builder.variant(string_schema);
        union_builder.variant(int_schema);
        let union = union_builder
            .build(&mut builder)
            .expect("failed to build int string union");

        builder
            .build(union)
            .expect("failed to build int string union schema")
    }

    fn int_schema() -> Schema {
        let builder = SchemaBuilder::new();
        let root = builder.int();

        builder.build(root).expect("failed to build int schema")
    }

    fn long_schema() -> Schema {
        let builder = SchemaBuilder::new();
        let root = builder.long();

        builder.build(root).expect("failed to build long schema")
    }

    fn boolean_schema() -> Schema {
        let builder = SchemaBuilder::new();
        let root = builder.boolean();

        builder.build(root).expect("failed to build boolean schema")
    }

    fn float_schema() -> Schema {
        let builder = SchemaBuilder::new();
        let root = builder.float();

        builder.build(root).expect("failed to build float schema")
    }

    fn double_schema() -> Schema {
        let builder = SchemaBuilder::new();
        let root = builder.double();

        builder.build(root).expect("failed to build double schema")
    }

    fn string_schema() -> Schema {
        let builder = SchemaBuilder::new();
        let root = builder.string();

        builder.build(root).expect("failed to build string schema")
    }

    fn bytes_schema() -> Schema {
        let builder = SchemaBuilder::new();
        let root = builder.bytes();

        builder.build(root).expect("failed to build bytes schema")
    }

    fn null_schema() -> Schema {
        let builder = SchemaBuilder::new();
        let root = builder.null();

        builder.build(root).expect("failed to build null schema")
    }

    #[test]
    fn test_broken() {
        assert!(!SchemaCompatibility::can_read(
            &int_string_union_schema().root(),
            &int_union_schema().root()
        ))
    }

    #[test]
    fn test_incompatible_reader_writer_pairs() {
        let incompatible_schemas = vec![
            // null
            (null_schema(), int_schema()),
            (null_schema(), long_schema()),
            // boolean
            (boolean_schema(), int_schema()),
            // int
            (int_schema(), null_schema()),
            (int_schema(), boolean_schema()),
            (int_schema(), long_schema()),
            (int_schema(), float_schema()),
            (int_schema(), double_schema()),
            // long
            (long_schema(), float_schema()),
            (long_schema(), double_schema()),
            // float
            (float_schema(), double_schema()),
            // string
            (string_schema(), boolean_schema()),
            (string_schema(), int_schema()),
            // bytes
            (bytes_schema(), null_schema()),
            (bytes_schema(), int_schema()),
            // array and maps
            (int_array_schema(), long_array_schema()),
            (int_map_schema(), int_array_schema()),
            (int_array_schema(), int_map_schema()),
            (int_map_schema(), long_map_schema()),
            // enum
            (enum1_ab_schema(), enum1_abc_schema()),
            (enum1_bc_schema(), enum1_abc_schema()),
            (enum1_ab_schema(), enum2_ab_schema()),
            (int_schema(), enum2_ab_schema()),
            (enum2_ab_schema(), int_schema()),
            //union
            (int_union_schema(), int_string_union_schema()),
            (string_union_schema(), int_string_union_schema()),
            //record
            (empty_record2_schema(), empty_record1_schema()),
            (a_int_record1_schema(), empty_record1_schema()),
            (a_int_b_dint_record1_schema(), empty_record1_schema()),
            (int_list_record_schema(), long_list_record_schema()),
            (nested_record(), nested_optional_record()),
        ];

        assert!(!incompatible_schemas
            .iter()
            .any(|(reader, writer)| SchemaCompatibility::can_read(&writer.root(), &reader.root())));
    }

    #[test]
    fn test_compatible_reader_writer_pairs() {
        let compatible_schemas = vec![
            (null_schema(), null_schema()),
            (long_schema(), int_schema()),
            (float_schema(), int_schema()),
            (float_schema(), long_schema()),
            (double_schema(), long_schema()),
            (double_schema(), int_schema()),
            (double_schema(), float_schema()),
            (string_schema(), bytes_schema()),
            (bytes_schema(), string_schema()),
            (int_array_schema(), int_array_schema()),
            (long_array_schema(), int_array_schema()),
            (int_map_schema(), int_map_schema()),
            (long_map_schema(), int_map_schema()),
            (enum1_ab_schema(), enum1_ab_schema()),
            (enum1_abc_schema(), enum1_ab_schema()),
            (empty_union_schema(), empty_union_schema()),
            (int_union_schema(), int_union_schema()),
            (int_string_union_schema(), string_int_union_schema()),
            (int_union_schema(), empty_union_schema()),
            (long_union_schema(), int_union_schema()),
            (int_union_schema(), int_schema()),
            (int_schema(), int_union_schema()),
            (empty_record1_schema(), empty_record1_schema()),
            (empty_record1_schema(), a_int_record1_schema()),
            (a_int_record1_schema(), a_int_record1_schema()),
            (a_dint_record1_schema(), a_int_record1_schema()),
            (a_dint_record1_schema(), a_dint_record1_schema()),
            (a_int_record1_schema(), a_dint_record1_schema()),
            (a_long_record1_schema(), a_int_record1_schema()),
            (a_int_record1_schema(), a_int_b_int_record1_schema()),
            (a_dint_record1_schema(), a_int_b_int_record1_schema()),
            (a_int_b_dint_record1_schema(), a_int_record1_schema()),
            (a_dint_b_dint_record1_schema(), empty_record1_schema()),
            (a_dint_b_dint_record1_schema(), a_int_record1_schema()),
            (a_int_b_int_record1_schema(), a_dint_b_dint_record1_schema()),
            (int_list_record_schema(), int_list_record_schema()),
            (long_list_record_schema(), long_list_record_schema()),
            (long_list_record_schema(), int_list_record_schema()),
            (nested_optional_record(), nested_record()),
        ];

        assert!(compatible_schemas
            .iter()
            .all(|(reader, writer)| SchemaCompatibility::can_read(&writer.root(), &reader.root())));
    }

    fn writer_schema() -> Schema {
        Schema::parse_str(
            r#"
      {"type":"record", "name":"Record", "fields":[
        {"name":"oldfield1", "type":"int"},
        {"name":"oldfield2", "type":"string"}
      ]}
"#,
        )
        .unwrap()
    }

    #[test]
    fn test_missing_field() {
        let reader_schema = Schema::parse_str(
            r#"
      {"type":"record", "name":"Record", "fields":[
        {"name":"oldfield1", "type":"int"}
      ]}
"#,
        )
        .unwrap();
        assert!(SchemaCompatibility::can_read(
            &writer_schema().root(),
            &reader_schema.root(),
        ));
        assert_eq!(
            SchemaCompatibility::can_read(&reader_schema.root(), &writer_schema().root()),
            false
        );
    }

    #[test]
    fn test_missing_second_field() {
        let reader_schema = Schema::parse_str(
            r#"
        {"type":"record", "name":"Record", "fields":[
          {"name":"oldfield2", "type":"string"}
        ]}
"#,
        )
        .unwrap();
        assert!(SchemaCompatibility::can_read(
            &writer_schema().root(),
            &reader_schema.root()
        ));
        assert_eq!(
            SchemaCompatibility::can_read(&reader_schema.root(), &writer_schema().root()),
            false
        );
    }

    #[test]
    fn test_all_fields() {
        let reader_schema = Schema::parse_str(
            r#"
        {"type":"record", "name":"Record", "fields":[
          {"name":"oldfield1", "type":"int"},
          {"name":"oldfield2", "type":"string"}
        ]}
"#,
        )
        .unwrap();
        assert!(SchemaCompatibility::can_read(
            &writer_schema().root(),
            &reader_schema.root()
        ));
        assert!(SchemaCompatibility::can_read(
            &reader_schema.root(),
            &writer_schema().root()
        ));
    }

    #[test]
    fn test_new_field_with_default() {
        let reader_schema = Schema::parse_str(
            r#"
        {"type":"record", "name":"Record", "fields":[
          {"name":"oldfield1", "type":"int"},
          {"name":"newfield1", "type":"int", "default":42}
        ]}
"#,
        )
        .unwrap();
        assert!(SchemaCompatibility::can_read(
            &writer_schema().root(),
            &reader_schema.root()
        ));
        assert_eq!(
            SchemaCompatibility::can_read(&reader_schema.root(), &writer_schema().root()),
            false
        );
    }

    #[test]
    fn test_new_field() {
        let reader_schema = Schema::parse_str(
            r#"
        {"type":"record", "name":"Record", "fields":[
          {"name":"oldfield1", "type":"int"},
          {"name":"newfield1", "type":"int"}
        ]}
"#,
        )
        .unwrap();
        assert_eq!(
            SchemaCompatibility::can_read(&writer_schema().root(), &reader_schema.root()),
            false
        );
        assert_eq!(
            SchemaCompatibility::can_read(&reader_schema.root(), &writer_schema().root()),
            false
        );
    }

    #[test]
    fn test_array_writer_schema() {
        let valid_reader = string_array_schema();
        let invalid_reader = string_map_schema();

        assert!(SchemaCompatibility::can_read(
            &string_array_schema().root(),
            &valid_reader.root()
        ));
        assert_eq!(
            SchemaCompatibility::can_read(&string_array_schema().root(), &invalid_reader.root()),
            false
        );
    }

    #[test]
    fn test_primitive_writer_schema() {
        let valid_reader = SchemaType::String;
        assert!(SchemaCompatibility::can_read(
            &SchemaType::String,
            &valid_reader
        ));
        assert_eq!(
            SchemaCompatibility::can_read(&SchemaType::Int, &SchemaType::String),
            false
        );
    }

    #[test]
    fn test_union_reader_writer_subset_incompatibility() {
        // reader union schema must contain all writer union branches

        let mut writer_builder = SchemaBuilder::new();
        let mut writer_union_builder = writer_builder.union();
        writer_union_builder.variant(writer_builder.null());
        writer_union_builder.variant(writer_builder.int());
        writer_union_builder.variant(writer_builder.string());
        let writer_root = writer_union_builder.build(&mut writer_builder).unwrap();
        let union_writer = writer_builder.build(writer_root).unwrap();

        let mut reader_builder = SchemaBuilder::new();
        let mut reader_union_builder = reader_builder.union();
        reader_union_builder.variant(reader_builder.null());
        reader_union_builder.variant(reader_builder.string());
        let reader_root = reader_union_builder.build(&mut reader_builder).unwrap();
        let union_reader = reader_builder.build(reader_root).unwrap();

        assert_eq!(
            SchemaCompatibility::can_read(&union_writer.root(), &union_reader.root()),
            false
        );
        assert!(SchemaCompatibility::can_read(
            &union_reader.root(),
            &union_writer.root()
        ));
    }

    #[test]
    fn test_incompatible_record_field() {
        let string_schema = Schema::parse_str(
            r#"
        {"type":"record", "name":"MyRecord", "namespace":"ns", "fields": [
            {"name":"field1", "type":"string"}
        ]}
        "#,
        )
        .unwrap();

        let int_schema = Schema::parse_str(
            r#"
      {"type":"record", "name":"MyRecord", "namespace":"ns", "fields": [
        {"name":"field1", "type":"int"}
      ]}
"#,
        )
        .unwrap();

        assert_eq!(
            SchemaCompatibility::can_read(&string_schema.root(), &int_schema.root()),
            false
        );
    }

    #[test]
    fn test_enum_symbols() {
        let enum_schema1 = Schema::parse_str(
            r#"
      {"type":"enum", "name":"MyEnum", "symbols":["A","B"]}
"#,
        )
        .unwrap();
        let enum_schema2 =
            Schema::parse_str(r#"{"type":"enum", "name":"MyEnum", "symbols":["A","B","C"]}"#)
                .unwrap();
        assert_eq!(
            SchemaCompatibility::can_read(&enum_schema2.root(), &enum_schema1.root()),
            false
        );
        assert!(SchemaCompatibility::can_read(
            &enum_schema1.root(),
            &enum_schema2.root()
        ));
    }

    // unused
    /*
        fn point_2d_schema() -> SchemaType {
            SchemaType::parse_str(
                r#"
          {"type":"record", "name":"Point2D", "fields":[
            {"name":"x", "type":"double"},
            {"name":"y", "type":"double"}
          ]}
        "#,
            )
            .unwrap()
        }
    */

    #[allow(dead_code)]
    //TODO: remove this function?
    fn point_2d_fullname_schema() -> Schema {
        Schema::parse_str(
            r#"
      {"type":"record", "name":"Point", "namespace":"written", "fields":[
        {"name":"x", "type":"double"},
        {"name":"y", "type":"double"}
      ]}
    "#,
        )
        .unwrap()
    }

    #[allow(dead_code)]
    // TODO: remove this function?
    fn point_3d_no_default_schema() -> Schema {
        Schema::parse_str(
            r#"
      {"type":"record", "name":"Point", "fields":[
        {"name":"x", "type":"double"},
        {"name":"y", "type":"double"},
        {"name":"z", "type":"double"}
      ]}
    "#,
        )
        .unwrap()
    }

    // unused
    /*
        fn point_3d_schema() -> SchemaType {
            SchemaType::parse_str(
                r#"
          {"type":"record", "name":"Point3D", "fields":[
            {"name":"x", "type":"double"},
            {"name":"y", "type":"double"},
            {"name":"z", "type":"double", "default": 0.0}
          ]}
        "#,
            )
            .unwrap()
        }

        fn point_3d_match_name_schema() -> SchemaType {
            SchemaType::parse_str(
                r#"
          {"type":"record", "name":"Point", "fields":[
            {"name":"x", "type":"double"},
            {"name":"y", "type":"double"},
            {"name":"z", "type":"double", "default": 0.0}
          ]}
        "#,
            )
            .unwrap()
        }
    */

    //  TODO:
    // #[test]
    // fn test_union_resolution_no_structure_match() {
    //     // short name match, but no structure match
    //     let read_schema = union_schema(vec![SchemaType::Null, point_3d_no_default_schema()]);
    //     assert_eq!(
    //         SchemaCompatibility::can_read(&point_2d_fullname_schema().root(), &read_schema.root()),
    //         false
    //     );
    // }

    // TODO(nlopes): the below require named schemas to be fully supported. See:
    // https://github.com/flavray/avro-rs/pull/76
    //
    // #[test]
    // fn test_union_resolution_first_structure_match_2d() {
    //     // multiple structure matches with no name matches
    //     let read_schema = union_schema(vec![
    //         SchemaType::Null,
    //         point_3d_no_default_schema(),
    //         point_2d_schema(),
    //         point_3d_schema(),
    //     ]);
    //     assert_eq!(
    //         SchemaCompatibility::can_read(&point_2d_fullname_schema(), &read_schema),
    //         false
    //     );
    // }

    // #[test]
    // fn test_union_resolution_first_structure_match_3d() {
    //     // multiple structure matches with no name matches
    //     let read_schema = union_schema(vec![
    //         SchemaType::Null,
    //         point_3d_no_default_schema(),
    //         point_3d_schema(),
    //         point_2d_schema(),
    //     ]);
    //     assert_eq!(
    //         SchemaCompatibility::can_read(&point_2d_fullname_schema(), &read_schema),
    //         false
    //     );
    // }

    // #[test]
    // fn test_union_resolution_named_structure_match() {
    //     // multiple structure matches with a short name match
    //     let read_schema = union_schema(vec![
    //         SchemaType::Null,
    //         point_2d_schema(),
    //         point_3d_match_name_schema(),
    //         point_3d_schema(),
    //     ]);
    //     assert_eq!(
    //         SchemaCompatibility::can_read(&point_2d_fullname_schema(), &read_schema),
    //         false
    //     );
    // }

    // #[test]
    // fn test_union_resolution_full_name_match() {
    //     // there is a full name match that should be chosen
    //     let read_schema = union_schema(vec![
    //         SchemaType::Null,
    //         point_2d_schema(),
    //         point_3d_match_name_schema(),
    //         point_3d_schema(),
    //         point_2d_fullname_schema(),
    //     ]);
    //     assert!(SchemaCompatibility::can_read(
    //         &point_2d_fullname_schema(),
    //         &read_schema
    //     ));
    // }
}
