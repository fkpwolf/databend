// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use common_base::base::tokio::io::AsyncWrite;
use common_datablocks::SendableDataBlockStream;
use common_datavalues::prelude::TypeID;
use common_datavalues::remove_nullable;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_datavalues::DateConverter;
use common_datavalues::TypeSerializerImpl;
use common_exception::ErrorCode;
use common_exception::Result;
use common_formats::field_encoder::FieldEncoderRowBased;
use common_formats::field_encoder::FieldEncoderValues;
use common_io::prelude::FormatSettings;
use futures_util::StreamExt;
use opensrv_mysql::*;
use tracing::error;

/// Reports progress information as string, intend to be put into the mysql Ok packet.
/// Mainly for decoupling with concrete type like `QueryContext`
///
/// Something like
/// "Read x rows, y MiB in z sec., A million rows/sec., B MiB/sec."
pub trait ProgressReporter {
    fn progress_info(&self) -> String;
}

pub struct QueryResult {
    blocks: SendableDataBlockStream,
    extra_info: Option<Box<dyn ProgressReporter + Send>>,
    has_result_set: bool,
    schema: DataSchemaRef,
}

impl QueryResult {
    pub fn create(
        blocks: SendableDataBlockStream,
        extra_info: Option<Box<dyn ProgressReporter + Send>>,
        has_result_set: bool,
        schema: DataSchemaRef,
    ) -> QueryResult {
        QueryResult {
            blocks,
            extra_info,
            has_result_set,
            schema,
        }
    }
}

pub struct DFQueryResultWriter<'a, W: AsyncWrite + Send + Unpin> {
    inner: Option<QueryResultWriter<'a, W>>,
}

fn write_field<'a, 'b, W: AsyncWrite + Unpin>(
    row_writer: &mut RowWriter<'b, W>,
    serializer: &TypeSerializerImpl<'a>,
    encoder: &FieldEncoderValues,
    buf: &mut Vec<u8>,
    row_index: usize,
) -> Result<()> {
    buf.clear();
    encoder.write_field(serializer, row_index, buf, true);
    row_writer.write_col(&buf[..])?;
    Ok(())
}

impl<'a, W: AsyncWrite + Send + Unpin> DFQueryResultWriter<'a, W> {
    pub fn create(inner: QueryResultWriter<'a, W>) -> DFQueryResultWriter<'a, W> {
        DFQueryResultWriter::<'a, W> { inner: Some(inner) }
    }

    pub async fn write(
        &mut self,
        query_result: Result<QueryResult>,
        format: &FormatSettings,
    ) -> Result<()> {
        if let Some(writer) = self.inner.take() {
            match query_result {
                Ok(query_result) => Self::ok(query_result, writer, format).await?,
                Err(error) => Self::err(&error, writer).await?,
            }
        }
        Ok(())
    }

    async fn ok(
        mut query_result: QueryResult,
        dataset_writer: QueryResultWriter<'a, W>,
        format: &FormatSettings,
    ) -> Result<()> {
        // XXX: num_columns == 0 may is error?
        if !query_result.has_result_set {
            // For statements without result sets, we still need to pull the stream because errors may occur in the stream.
            let blocks = &mut query_result.blocks;
            while let Some(block) = blocks.next().await {
                if let Err(e) = block {
                    error!("dataset write failed: {:?}", e);
                    dataset_writer
                        .error(
                            ErrorKind::ER_UNKNOWN_ERROR,
                            format!("dataset write failed: {}", e).as_bytes(),
                        )
                        .await?;

                    return Ok(());
                }
            }

            dataset_writer.completed(OkResponse::default()).await?;
            return Ok(());
        }

        fn convert_field_type(field: &DataField) -> Result<ColumnType> {
            match remove_nullable(field.data_type()).data_type_id() {
                TypeID::Int8 => Ok(ColumnType::MYSQL_TYPE_TINY),
                TypeID::Int16 => Ok(ColumnType::MYSQL_TYPE_SHORT),
                TypeID::Int32 => Ok(ColumnType::MYSQL_TYPE_LONG),
                TypeID::Int64 => Ok(ColumnType::MYSQL_TYPE_LONGLONG),
                TypeID::UInt8 => Ok(ColumnType::MYSQL_TYPE_TINY),
                TypeID::UInt16 => Ok(ColumnType::MYSQL_TYPE_SHORT),
                TypeID::UInt32 => Ok(ColumnType::MYSQL_TYPE_LONG),
                TypeID::UInt64 => Ok(ColumnType::MYSQL_TYPE_LONGLONG),
                TypeID::Float32 => Ok(ColumnType::MYSQL_TYPE_FLOAT),
                TypeID::Float64 => Ok(ColumnType::MYSQL_TYPE_DOUBLE),
                TypeID::String => Ok(ColumnType::MYSQL_TYPE_VARCHAR),
                TypeID::Boolean => Ok(ColumnType::MYSQL_TYPE_SHORT),
                TypeID::Date => Ok(ColumnType::MYSQL_TYPE_DATE),
                TypeID::Timestamp => Ok(ColumnType::MYSQL_TYPE_DATETIME),
                TypeID::Null => Ok(ColumnType::MYSQL_TYPE_NULL),
                TypeID::Interval => Ok(ColumnType::MYSQL_TYPE_LONGLONG),
                TypeID::Array => Ok(ColumnType::MYSQL_TYPE_VARCHAR),
                TypeID::Struct => Ok(ColumnType::MYSQL_TYPE_VARCHAR),
                TypeID::Variant => Ok(ColumnType::MYSQL_TYPE_VARCHAR),
                TypeID::VariantArray => Ok(ColumnType::MYSQL_TYPE_VARCHAR),
                TypeID::VariantObject => Ok(ColumnType::MYSQL_TYPE_VARCHAR),
                _ => Err(ErrorCode::Unimplemented(format!(
                    "Unsupported column type:{:?}",
                    field.data_type()
                ))),
            }
        }

        fn make_column_from_field(field: &DataField) -> Result<Column> {
            convert_field_type(field).map(|column_type| Column {
                table: "".to_string(),
                column: field.name().to_string(),
                coltype: column_type,
                colflags: ColumnFlags::empty(),
            })
        }

        fn convert_schema(schema: &DataSchemaRef) -> Result<Vec<Column>> {
            schema.fields().iter().map(make_column_from_field).collect()
        }

        let tz = format.timezone;
        match convert_schema(&query_result.schema) {
            Err(error) => Self::err(&error, dataset_writer).await,
            Ok(columns) => {
                let mut row_writer = dataset_writer.start(&columns).await?;

                let blocks = &mut query_result.blocks;
                while let Some(block) = blocks.next().await {
                    let block = match block {
                        Err(e) => {
                            error!("result row write failed: {:?}", e);
                            row_writer
                                .finish_error(
                                    ErrorKind::ER_UNKNOWN_ERROR,
                                    &format!("result row write failed: {}", e).as_bytes(),
                                )
                                .await?;
                            return Ok(());
                        }
                        Ok(block) => block,
                    };

                    match block.get_serializers() {
                        Ok(serializers) => {
                            let rows_size = block.column(0).len();
                            let encoder =
                                FieldEncoderValues::create_for_mysql_handler(format.timezone);
                            let mut buf = Vec::<u8>::new();
                            for row_index in 0..rows_size {
                                for (col_index, serializer) in serializers.iter().enumerate() {
                                    let val = block.column(col_index).get_checked(row_index)?;
                                    if val.is_null() {
                                        row_writer.write_col(None::<u8>)?;
                                        continue;
                                    }
                                    let data_type = remove_nullable(
                                        block.schema().fields()[col_index].data_type(),
                                    );

                                    match (data_type.data_type_id(), val.clone()) {
                                        (TypeID::Boolean, DataValue::Boolean(v)) => {
                                            row_writer.write_col(v as i8)?
                                        }
                                        (TypeID::Date, DataValue::Int64(v)) => {
                                            let v = v as i32;
                                            row_writer.write_col(v.to_date(&tz).naive_local())?
                                        }
                                        (TypeID::Timestamp, DataValue::Int64(_)) => write_field(
                                            &mut row_writer,
                                            serializer,
                                            &encoder,
                                            &mut buf,
                                            row_index,
                                        )?,
                                        (TypeID::String, DataValue::String(v)) => {
                                            row_writer.write_col(v)?
                                        }
                                        (TypeID::Array, DataValue::Array(_)) => write_field(
                                            &mut row_writer,
                                            serializer,
                                            &encoder,
                                            &mut buf,
                                            row_index,
                                        )?,
                                        (TypeID::Struct, DataValue::Struct(_)) => write_field(
                                            &mut row_writer,
                                            serializer,
                                            &encoder,
                                            &mut buf,
                                            row_index,
                                        )?,
                                        (TypeID::Variant, DataValue::Variant(_)) => write_field(
                                            &mut row_writer,
                                            serializer,
                                            &encoder,
                                            &mut buf,
                                            row_index,
                                        )?,
                                        (TypeID::VariantArray, DataValue::Variant(_)) => {
                                            write_field(
                                                &mut row_writer,
                                                serializer,
                                                &encoder,
                                                &mut buf,
                                                row_index,
                                            )?
                                        }
                                        (TypeID::VariantObject, DataValue::Variant(_)) => {
                                            write_field(
                                                &mut row_writer,
                                                serializer,
                                                &encoder,
                                                &mut buf,
                                                row_index,
                                            )?
                                        }
                                        (_, DataValue::Int64(v)) => row_writer.write_col(v)?,

                                        (_, DataValue::UInt64(v)) => row_writer.write_col(v)?,

                                        (_, DataValue::Float64(_)) =>
                                        // mysql writer use a text protocol,
                                        // it use format!() to serialize number,
                                        // the result will be different with our serializer for floats
                                        {
                                            buf.clear();
                                            encoder
                                                .write_field(serializer, row_index, &mut buf, true);
                                            row_writer.write_col(&buf[..])?;
                                        }
                                        (_, v) => {
                                            return Err(ErrorCode::BadDataValueType(format!(
                                                "Unsupported column type:{:?}, expected type in schema: {:?}",
                                                v.data_type(),
                                                data_type
                                            )));
                                        }
                                    }
                                }
                                row_writer.end_row().await?;
                            }
                        }
                        Err(e) => {
                            error!("result row write failed: {:?}", e);
                            row_writer
                                .finish_error(
                                    ErrorKind::ER_UNKNOWN_ERROR,
                                    &format!("result row write failed: {}", e).as_bytes(),
                                )
                                .await?;
                            return Ok(());
                        }
                    }
                }

                let info = query_result
                    .extra_info
                    .map(|r| r.progress_info())
                    .unwrap_or_default();
                row_writer.finish_with_info(&info).await?;

                Ok(())
            }
        }
    }

    async fn err(error: &ErrorCode, writer: QueryResultWriter<'a, W>) -> Result<()> {
        if error.code() != ErrorCode::ABORTED_QUERY && error.code() != ErrorCode::ABORTED_SESSION {
            error!("OnQuery Error: {:?}", error);
            writer
                .error(ErrorKind::ER_UNKNOWN_ERROR, error.to_string().as_bytes())
                .await?;
        } else {
            writer
                .error(
                    ErrorKind::ER_ABORTING_CONNECTION,
                    error.to_string().as_bytes(),
                )
                .await?;
        }

        Ok(())
    }
}
