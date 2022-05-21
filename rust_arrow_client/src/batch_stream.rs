use crate::array::{fixed_size_array, string_array};
use crate::data_model::{Column, Type};
use crate::Table;
use anyhow::bail;
use arrow::array::ArrayRef;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use serde_json::json;
use static_assertions::const_assert;
use std::io::Read;

const MAX_BATCH_LEN: u32 = 1 << 14;

// using i32 string offset arrays impose this batch length limit with max string size of u16::MAX
const_assert!(MAX_BATCH_LEN as u128 * (u16::MAX as u128) <= i32::MAX as u128);

pub struct BatchStream {
    stream: Box<dyn Read + Send>,
    columns: Vec<Column>,
    schema: SchemaRef,
}

impl BatchStream {
    pub fn start(base_url: &str, table_name: &str, table: &Table) -> anyhow::Result<Self> {
        let url = base_url.to_string() + "/data-stream";
        let columns = table.columns().to_vec();
        let column_names: Vec<&str> = columns.iter().map(|c| c.name()).collect();
        let response = ureq::post(&url).send_json(&json!({
            "table": table_name,
            "fields": column_names,
            "batch": MAX_BATCH_LEN
        }))?;
        if response.status() != 200 {
            bail!("response status: {} != 200", response.status())
        }
        Ok(Self {
            stream: Box::new(response.into_reader()),
            columns,
            schema: SchemaRef::new(table.arrow_schema()),
        })
    }
    fn read_batch(&mut self) -> anyhow::Result<RecordBatch> {
        let mut batch_len = [0u8; 4];
        self.stream.read_exact(&mut batch_len)?;
        let batch_len = u32::from_le_bytes(batch_len);
        if batch_len == 0 {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        } else if batch_len > MAX_BATCH_LEN {
            bail!("max batch len exceeded")
        }

        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.columns.len());
        for column in &self.columns {
            let array = match column.typ() {
                Type::String => string_array(&mut self.stream, batch_len),
                typ => fixed_size_array(&mut self.stream, typ, batch_len),
            }?;
            arrays.push(ArrayRef::from(array));
        }
        let record_batch = RecordBatch::try_new(self.schema.clone(), arrays)?;
        Ok(record_batch)
    }
}

impl Iterator for BatchStream {
    type Item = arrow::error::Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        let batch = match self.read_batch() {
            Ok(b) => b,
            Err(err) => return Some(Err(ArrowError::ExternalError(err.into()))),
        };
        if batch.num_rows() == 0 {
            return None;
        }
        Some(Ok(batch))
    }
}
