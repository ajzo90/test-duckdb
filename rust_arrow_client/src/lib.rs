extern crate core;

mod array;
mod batch_stream;
mod data_model;

use libc::c_char;
use std::ffi::CStr;
use crate::batch_stream::BatchStream;
use crate::data_model::{DataModel, Table};
use arrow::datatypes::SchemaRef;
use arrow::error::Result;
use arrow::ffi::FFI_ArrowSchema;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow::record_batch::{RecordBatch, RecordBatchReader};

#[no_mangle]
pub extern "C" fn get_arrow_array_stream(base_url: *const c_char, table_name: *const c_char) -> FFI_ArrowArrayStream {
    let (base_url, table_name) = unsafe {
        (CStr::from_ptr(base_url).to_str().unwrap(),
         CStr::from_ptr(table_name).to_str().unwrap())
    };
    let data_model = DataModel::get(base_url).unwrap();
    let table = data_model.table(table_name).unwrap();
    let batch_reader = IteratorImpl::new(&base_url, table, table_name).unwrap();
    FFI_ArrowArrayStream::new(Box::new(batch_reader))
}

#[no_mangle]
pub extern "C" fn get_arrow_array_schema(base_url: *const c_char, table_name: *const c_char) -> FFI_ArrowSchema {
    let (base_url, table_name) = unsafe {
        (CStr::from_ptr(base_url).to_str().unwrap(),
         CStr::from_ptr(table_name).to_str().unwrap())
    };
    let data_model = DataModel::get(base_url).unwrap();
    let table = data_model.table(table_name).unwrap();
    FFI_ArrowSchema::try_from(table.arrow_schema()).unwrap()
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
