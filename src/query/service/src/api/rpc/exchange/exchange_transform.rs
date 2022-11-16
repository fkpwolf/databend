// Copyright 2022 Datafuse Labs.
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

use std::any::Any;
use std::sync::Arc;

use common_arrow::arrow::io::flight::default_ipc_fields;
use common_arrow::arrow::io::flight::deserialize_batch;
use common_arrow::arrow::io::flight::serialize_batch;
use common_arrow::arrow::io::ipc::IpcSchema;
use common_catalog::table_context::TableContext;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use common_pipeline_core::Pipeline;

use crate::api::rpc::exchange::exchange_params::ExchangeParams;
use crate::api::rpc::exchange::exchange_params::SerializeParams;
use crate::api::rpc::exchange::exchange_params::ShuffleExchangeParams;
use crate::api::rpc::exchange::exchange_transform_source::ExchangeSourceTransform;
use crate::api::rpc::flight_client::FlightExchange;
use crate::api::DataPacket;
use crate::api::FragmentData;
use crate::clusters::ClusterHelper;
use crate::sessions::QueryContext;

struct OutputData {
    pub data_block: Option<DataBlock>,
    pub has_serialized_blocks: bool,
    pub serialized_blocks: Vec<Option<DataPacket>>,
}

pub struct ExchangeTransform {
    finished: bool,
    wait_channel_closed: bool,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    input_data: Option<DataBlock>,
    remote_data: Option<DataPacket>,
    output_data: Option<OutputData>,
    flight_exchanges: Vec<FlightExchange>,
    serialize_params: SerializeParams,
    shuffle_exchange_params: ShuffleExchangeParams,
}

impl ExchangeTransform {
    fn try_create(
        ctx: Arc<QueryContext>,
        params: &ShuffleExchangeParams,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
    ) -> Result<ProcessorPtr> {
        let exchange_params = ExchangeParams::ShuffleExchange(params.clone());
        let exchange_manager = ctx.get_exchange_manager();
        let flight_exchanges = exchange_manager.get_flight_exchanges(&exchange_params)?;

        Ok(ProcessorPtr::create(Box::new(ExchangeTransform {
            input,
            output,
            flight_exchanges,
            finished: false,
            input_data: None,
            remote_data: None,
            output_data: None,
            shuffle_exchange_params: params.clone(),
            serialize_params: params.create_serialize_params()?,
            wait_channel_closed: false,
        })))
    }

    pub fn via(
        ctx: &Arc<QueryContext>,
        params: &ExchangeParams,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        match params {
            ExchangeParams::MergeExchange(params) => {
                if params.destination_id != ctx.get_cluster().local_id() {
                    // transfter to myself?
                    return Err(ErrorCode::Internal(format!(
                        "Locally depends on merge exchange, but the localhost is not a coordination node. executor: {}, destination_id: {}, fragment id: {}",
                        ctx.get_cluster().local_id(),
                        params.destination_id,
                        params.fragment_id
                    )));
                }

                pipeline.add_transform(|transform_input_port, transform_output_port| {
                    ExchangeSourceTransform::try_create(
                        ctx,
                        transform_input_port,
                        transform_output_port,
                        params,
                    )
                })
            }
            ExchangeParams::ShuffleExchange(params) => {
                pipeline.add_transform(|transform_input_port, transform_output_port| {
                    ExchangeTransform::try_create(
                        ctx.clone(),
                        params,
                        transform_input_port,
                        transform_output_port,
                    )
                })
            }
        }
    }

    async fn send_packet(output_packet: DataPacket, exchange: FlightExchange) -> Result<()> {
        if exchange.send(output_packet).await.is_err() {
            return Err(ErrorCode::TokioError(
                "Cannot send flight data to endpoint, because sender is closed.",
            ));
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl Processor for ExchangeTransform {
    fn name(&self) -> String {
        "ExchangeTransform".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        // This may cause other cluster nodes to idle.
        if !self.output.can_push() {
            // TODO: try send data if other nodes can recv data.
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        // If data needs to be sent to other nodes.
        if let Some(mut output_data) = self.output_data.take() {
            if let Some(data_block) = output_data.data_block.take() {
                self.output.push_data(Ok(data_block));
            }

            self.output_data = Some(output_data);
            return Ok(Event::Async);
        }

        if self.input_data.is_some() || self.remote_data.is_some() {
            return Ok(Event::Sync);
        }

        // If the data of other nodes can be received.
        for flight_exchange in &self.flight_exchanges {
            if let Some(data_packet) = flight_exchange.try_recv()? {
                self.remote_data = Some(data_packet);
                return Ok(Event::Sync);
            }
        }

        if self.input.is_finished() {
            if self.finished {
                self.output.finish();
                return Ok(Event::Finished);
            }

            if !self.wait_channel_closed {
                self.wait_channel_closed = true;

                for flight_exchange in &self.flight_exchanges {
                    // No more data will be sent. close the response of endpoint.
                    flight_exchange.close_output();
                }
            }

            return Ok(Event::Async);
        }

        if self.input.has_data() {
            self.input_data = Some(self.input.pull_data().unwrap()?);
            return Ok(Event::Sync);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        // Prepare data to be sent to other nodes
        if let Some(data_block) = self.input_data.take() {
            let scatter = &self.shuffle_exchange_params.shuffle_scatter;

            let scatted_blocks = scatter.execute(&data_block, 0)?;

            let mut output_data = OutputData {
                data_block: None,
                serialized_blocks: vec![],
                has_serialized_blocks: false,
            };

            for (index, data_block) in scatted_blocks.into_iter().enumerate() {
                if data_block.is_empty() {
                    output_data.serialized_blocks.push(None);
                    continue;
                }

                if index == self.serialize_params.local_executor_pos {
                    output_data.data_block = Some(data_block);
                    output_data.serialized_blocks.push(None);
                } else {
                    let meta = match bincode::serialize(&data_block.meta()?) {
                        Ok(bytes) => Ok(bytes),
                        Err(_) => Err(ErrorCode::BadBytes(
                            "block meta serialize error when exchange",
                        )),
                    }?;

                    let chunks = data_block.try_into()?;
                    let options = &self.serialize_params.options;
                    let ipc_fields = &self.serialize_params.ipc_fields;
                    let (dicts, values) = serialize_batch(&chunks, ipc_fields, options)?;

                    if !dicts.is_empty() {
                        return Err(ErrorCode::Unimplemented(
                            "DatabendQuery does not implement dicts.",
                        ));
                    }

                    output_data.has_serialized_blocks = true;
                    let data = FragmentData::create(meta, values);
                    output_data
                        .serialized_blocks
                        .push(Some(DataPacket::FragmentData(data)));
                }
            }

            self.output_data = Some(output_data);
        }

        // Processing data received from other nodes
        if let Some(remote_data) = self.remote_data.take() {
            return match remote_data {
                DataPacket::ErrorCode(v) => self.on_recv_error(v),
                DataPacket::ProgressAndPrecommit { .. } => unreachable!(),
                DataPacket::FetchProgressAndPrecommit => unreachable!(),
                DataPacket::FragmentData(v) => self.on_recv_data(v),
            };
        }

        Ok(())
    }

    async fn async_process(&mut self) -> Result<()> {
        if let Some(mut output_data) = self.output_data.take() {
            let mut futures = Vec::with_capacity(output_data.serialized_blocks.len());
            for index in 0..output_data.serialized_blocks.len() {
                if let Some(output_packet) = output_data.serialized_blocks[index].take() {
                    let exchange = self.flight_exchanges[index].clone();
                    futures.push(Self::send_packet(output_packet, exchange));
                }
            }

            futures::future::try_join_all(futures).await?;
        }

        if self.wait_channel_closed && !self.finished {
            // async recv if input is finished.
            // TODO: use future::future::select_all to parallel await
            for flight_exchange in &self.flight_exchanges {
                if let Some(data_packet) = flight_exchange.recv().await? {
                    self.remote_data = Some(data_packet);
                    return Ok(());
                }
            }

            self.finished = true;
        }

        Ok(())
    }
}

impl ExchangeTransform {
    fn on_recv_error(&mut self, cause: ErrorCode) -> Result<()> {
        Err(cause)
    }

    fn on_recv_data(&mut self, fragment_data: FragmentData) -> Result<()> {
        // do nothing if has output data.
        if self.output_data.is_some() {
            self.remote_data = Some(DataPacket::FragmentData(fragment_data));
            return Ok(());
        }

        let schema = &self.shuffle_exchange_params.schema;

        let arrow_schema = Arc::new(schema.to_arrow());
        let ipc_fields = default_ipc_fields(&arrow_schema.fields);
        let ipc_schema = IpcSchema {
            fields: ipc_fields,
            is_little_endian: true,
        };

        let batch = deserialize_batch(
            &fragment_data.data,
            &arrow_schema.fields,
            &ipc_schema,
            &Default::default(),
        )?;

        let meta = match bincode::deserialize(fragment_data.get_meta()) {
            Ok(meta) => Ok(meta),
            Err(cause) => Err(ErrorCode::BadBytes(format!(
                "block meta deserialize error when exchange, {:?}",
                cause
            ))),
        }?;

        self.output_data = Some(OutputData {
            serialized_blocks: vec![],
            has_serialized_blocks: false,
            data_block: Some(DataBlock::from_chunk(schema, &batch)?.add_meta(meta)?),
        });

        Ok(())
    }
}
