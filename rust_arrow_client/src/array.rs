use crate::data_model::Type;
use arrow::array::ArrayData;
use arrow::buffer::{Buffer, MutableBuffer};
use bytemuck::cast_slice_mut;
use std::io::Read;

pub fn fixed_size_array(
    stream: &mut dyn Read,
    typ: &Type,
    batch_len: u32,
) -> anyhow::Result<ArrayData> {
    let buffer_size = typ.element_size() * batch_len as usize;
    let mut buffer = MutableBuffer::from_len_zeroed(buffer_size);
    stream.read_exact(buffer.as_mut())?;
    let array = ArrayData::try_new(
        typ.arrow_data_type(),
        batch_len as usize,
        None,
        None,
        0,
        vec![Buffer::from(buffer)],
        vec![],
    )?;
    Ok(array)
}

pub fn string_array(stream: &mut dyn Read, batch_len: u32) -> anyhow::Result<ArrayData> {
    let mut lengths = vec![0u16; batch_len as usize];
    stream.read_exact(cast_slice_mut(&mut lengths))?;
    let offset_buffer_size = 4usize * (1usize + batch_len as usize);
    let mut offset_buffer = MutableBuffer::from_len_zeroed(offset_buffer_size);
    let data_buffer_size: usize = unsafe {
        let offset_buffer: &mut [i32] = offset_buffer.typed_data_mut();
        let mut sum = 0i32;
        for (off, len) in offset_buffer[1..].iter_mut().zip(lengths) {
            sum += len as i32;
            *off = sum;
        }
        sum as usize
    };
    let mut data_buffer = MutableBuffer::from_len_zeroed(data_buffer_size);
    stream.read_exact(data_buffer.as_mut())?;
    let array = ArrayData::try_new(
        Type::String.arrow_data_type(),
        batch_len as usize,
        None,
        None,
        0,
        vec![Buffer::from(offset_buffer), Buffer::from(data_buffer)],
        vec![],
    )?;
    Ok(array)
}
