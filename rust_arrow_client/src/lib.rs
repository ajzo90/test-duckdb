mod array;
mod batch_stream;
mod data_model;

use crate::batch_stream::BatchStream;
use crate::data_model::{DataModel, Table};
use arrow::datatypes::SchemaRef;
use arrow::error::Result;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow::record_batch::{RecordBatch, RecordBatchReader};

#[no_mangle]
pub extern "C" fn get_arrow_array_stream() -> FFI_ArrowArrayStream {
    let base_url = "http://localhost:6789";
    let data_model = DataModel::get(base_url).unwrap();
    const TABLE_NAME: &str = "users";
    let table = data_model.table(TABLE_NAME).unwrap();
    let batch_reader = IteratorImpl::new(base_url, table, TABLE_NAME).unwrap();
    FFI_ArrowArrayStream::new(Box::new(batch_reader))
}

struct IteratorImpl {
    schema: SchemaRef,
    stream: BatchStream,
}

impl IteratorImpl {
    fn new(base_url: &str, table: &Table, table_name: &str) -> anyhow::Result<Self> {
        let base_url = base_url.to_string();
        let stream = BatchStream::start(&base_url, table_name, table)?;
        Ok(Self {
            schema: SchemaRef::from(table.arrow_schema()),
            stream,
        })
    }
}

impl RecordBatchReader for IteratorImpl {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Iterator for IteratorImpl {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.stream.next()
    }
}
