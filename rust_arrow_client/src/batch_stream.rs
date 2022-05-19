use crate::data_model::Column;
use crate::Table;
use anyhow::bail;
use arrow::array::{ArrayData, ArrayRef};
use arrow::buffer::{Buffer, MutableBuffer};
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use serde_json::json;
use std::io::Read;

const MAX_BATCH_LEN: u32 = 100000;

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
            let buffer_size = column.typ().element_size() * batch_len as usize;
            let mut buffer = MutableBuffer::from_len_zeroed(buffer_size);
            self.stream.read_exact(buffer.as_mut())?;
            let array = ArrayData::try_new(
                column.typ().arrow_data_type(),
                batch_len as usize,
                None,
                None,
                0,
                vec![Buffer::from(buffer)],
                vec![],
            )?;
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
