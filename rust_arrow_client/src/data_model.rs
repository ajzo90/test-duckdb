use arrow::datatypes::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(transparent)]
pub struct DataModel {
    tables: BTreeMap<String, Table>,
}

impl DataModel {
    pub fn get(base_url: &str) -> anyhow::Result<Self> {
        let url = base_url.to_string() + "/data-model";
        Ok(ureq::get(&url).call()?.into_json()?)
    }
    pub fn table(&self, name: &str) -> Option<&Table> {
        self.tables.get(name)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(transparent)]
pub struct Table {
    columns: Vec<Column>,
}

impl Table {
    pub fn arrow_schema(&self) -> Schema {
        let mut arrow_fields = Vec::new();
        arrow_fields.reserve(self.columns.len());
        for column in &self.columns {
            arrow_fields.push(column.arrow_field())
        }
        Schema::new(arrow_fields)
    }
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }
    pub fn column(&self, name: &str) -> &Column {
        self.columns
            .iter()
            .filter(|c| c.name() == name)
            .next()
            .unwrap()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct Column {
    name: String,
    #[serde(rename = "Type")]
    typ: Type,
}

impl Column {
    pub fn arrow_field(&self) -> Field {
        Field::new(&self.name, self.typ.arrow_data_type(), false)
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn typ(&self) -> &Type {
        &self.typ
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "lowercase")]
pub enum Type {
    U32,
    U16,
    U8,
    I32,
    I16,
    I8,
    String,
}

impl Type {
    pub fn arrow_data_type(&self) -> DataType {
        match self {
            Type::U32 => DataType::UInt32,
            Type::I32 => DataType::Int32,
            Type::U16 => DataType::UInt16,
            Type::I16 => DataType::Int16,
            Type::U8 => DataType::UInt8,
            Type::I8 => DataType::Int8,
            Type::String => DataType::Utf8,
        }
    }
    pub fn element_size(&self) -> usize {
        match self {
            Type::U32 | Type::I32 => 4,
            Type::U16 | Type::I16 => 2,
            Type::U8 | Type::I8 => 1,
            Type::String => 2,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::data_model::DataModel;

    #[test]
    fn json() {
        let _data_model: DataModel = serde_json::from_str(
            r#"
            {
                "transactions":[
                    {"Name":"id","Type":"u32"},
                    {"Name":"user","Type":"u32"},
                    {"Name":"item","Type":"u32"}
                ],
                "users":[
                    {"Name":"id","Type":"u32"},
                    {"Name":"age","Type":"u8"},
                    {"Name":"name","Type":"string"}
                ]
            }
        "#,
        )
        .unwrap();
    }
}
