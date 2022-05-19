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
    U8,
    String,
}

impl Type {
    pub fn arrow_data_type(&self) -> DataType {
        match self {
            Type::U32 => DataType::UInt32,
            Type::U8 => DataType::UInt8,
            Type::String => DataType::Utf8,
        }
    }
    pub fn element_size(&self) -> usize {
        match self {
            Type::U32 => 4,
            Type::U8 => 1,
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
