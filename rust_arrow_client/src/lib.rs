use arrow::array::{ArrayData, ArrayRef, StructArray};
use arrow::buffer::Buffer;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::Result;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow::record_batch::{RecordBatch, RecordBatchReader};
use std::cmp::min;

#[no_mangle]
pub extern "C" fn get_arrow_array_stream() -> FFI_ArrowArrayStream {
    FFI_ArrowArrayStream::new(Box::new(IteratorImpl::new()))
}

struct IteratorImpl {
    cnt: usize,
    schema: SchemaRef,
}

impl IteratorImpl {
    fn new() -> Self {
        Self {
            cnt: 10,
            schema: SchemaRef::new(Schema::new(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Int32, false),
            ])),
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
