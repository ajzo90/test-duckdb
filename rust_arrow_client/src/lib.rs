mod data_model;

use crate::data_model::{DataModel, Table};
use arrow::array::{ArrayData, ArrayRef, StructArray};
use arrow::buffer::Buffer;
use arrow::datatypes::{Field, SchemaRef};
use arrow::error::Result;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow::record_batch::{RecordBatch, RecordBatchReader};
use std::cmp::min;

#[no_mangle]
pub extern "C" fn get_arrow_array_stream() -> FFI_ArrowArrayStream {
    let base_url = "http://localhost:6789";
    let data_model = DataModel::get(base_url).unwrap();
    let table = data_model.table("transactions").unwrap();
    FFI_ArrowArrayStream::new(Box::new(IteratorImpl::new(base_url, table)))
}

struct IteratorImpl {
    cnt: usize,
    base_url: String,
    table: Table,
    schema: SchemaRef,
}

impl IteratorImpl {
    fn new(base_url: &str, table: &Table) -> Self {
        let table = table.clone();
        let schema = SchemaRef::new(table.arrow_schema());
        let base_url = base_url.to_string();
        Self {
            cnt: 10,
            base_url,
            table,
            schema,
        }
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
        if self.cnt == 0 {
            return None;
        }
        let l = min(self.cnt, 3);
        let fields: Vec<(Field, ArrayRef)> = self
            .schema()
            .fields()
            .iter()
            .map(|field| {
                let buffer = Buffer::from_slice_ref(&[1i32, 2i32, 3i32]);
                let array_data = ArrayData::builder(field.data_type().clone())
                    .len(l)
                    .add_buffer(buffer)
                    .build()
                    .unwrap();
                (field.clone(), ArrayRef::from(array_data))
            })
            .collect();
        self.cnt -= l;
        Some(Ok(RecordBatch::from(&StructArray::from(fields))))
    }
}
