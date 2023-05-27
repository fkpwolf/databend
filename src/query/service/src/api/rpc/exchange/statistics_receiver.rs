// Copyright 2021 Datafuse Labs
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

use std::collections::HashMap;
use std::sync::Arc;

use common_base::base::tokio::sync::broadcast::channel;
use common_base::base::tokio::sync::broadcast::Sender;
use common_base::base::tokio::task::JoinHandle;
use common_base::runtime::Runtime;
use common_base::runtime::TrySpawn;
use common_exception::ErrorCode;
use common_exception::Result;
use futures_util::future::select;
use futures_util::future::Either;

use crate::api::rpc::flight_client::FlightExchange;
use crate::api::DataPacket;
use crate::sessions::QueryContext;

pub struct StatisticsReceiver {
    _runtime: Runtime,
    shutdown_tx: Option<Sender<bool>>,
    exchange_handler: Vec<JoinHandle<Result<()>>>,
}

impl StatisticsReceiver {
    pub fn spawn_receiver(
        ctx: &Arc<QueryContext>,
        statistics_exchanges: HashMap<String, FlightExchange>,
    ) -> Result<StatisticsReceiver> {
        let (shutdown_tx, _shutdown_rx) = channel(2);
        let mut exchange_handler = Vec::with_capacity(statistics_exchanges.len());
        let runtime = Runtime::with_worker_threads(2, Some(String::from("StatisticsReceiver")))?;

        for (_source, exchange) in statistics_exchanges.into_iter() {
            let rx = exchange.convert_to_receiver();
            exchange_handler.push(runtime.spawn({
                let ctx = ctx.clone();
                let shutdown_rx = shutdown_tx.subscribe();

                async move {
                    let mut shutdown_rx = shutdown_rx;
                    let mut recv = Box::pin(rx.recv());
                    let mut notified = Box::pin(shutdown_rx.recv());

                    loop {
                        match select(notified, recv).await {
                            Either::Left((Ok(true /* has error */), _))
                            | Either::Left((Err(_), _)) => {
                                return Ok(());
                            }
                            Either::Left((Ok(false), recv)) => {
                                match StatisticsReceiver::recv_data(&ctx, recv.await) {
                                    Ok(true) => {
                                        return Ok(());
                                    }
                                    Err(cause) => {
                                        ctx.get_current_session().force_kill_query(cause.clone());
                                        return Err(cause);
                                    }
                                    _ => loop {
                                        match StatisticsReceiver::recv_data(&ctx, rx.recv().await) {
                                            Ok(true) => {
                                                return Ok(());
                                            }
                                            Err(cause) => {
                                                ctx.get_current_session()
                                                    .force_kill_query(cause.clone());
                                                return Err(cause);
                                            }
                                            _ => {}
                                        }
                                    },
                                }
                            }
                            Either::Right((res, left)) => {
                                match StatisticsReceiver::recv_data(&ctx, res) {
                                    Ok(true) => {
                                        return Ok(());
                                    }
                                    Ok(false) => {
                                        notified = left;
                                        recv = Box::pin(rx.recv());
                                    }
                                    Err(cause) => {
                                        ctx.get_current_session().force_kill_query(cause.clone());
                                        return Err(cause);
                                    }
                                };
                            }
                        }
                    }
                }
            }));
        }

        Ok(StatisticsReceiver {
            exchange_handler,
            _runtime: runtime,
            shutdown_tx: Some(shutdown_tx),
        })
    }

    fn recv_data(ctx: &Arc<QueryContext>, recv_data: Result<Option<DataPacket>>) -> Result<bool> {
        match recv_data {
            Ok(None) => Ok(true),
            Err(transport_error) => Err(transport_error),
            Ok(Some(DataPacket::ErrorCode(error))) => Err(error),
            Ok(Some(DataPacket::Dictionary(_))) => unreachable!(),
            Ok(Some(DataPacket::FragmentData(_))) => unreachable!(),
            Ok(Some(DataPacket::FetchProgressAndPrecommit)) => unreachable!(),
            Ok(Some(DataPacket::ProgressAndPrecommit {
                progress,
                precommit,
            })) => {
                for progress_info in progress {
                    progress_info.inc(ctx);
                }

                for precommit_block in precommit {
                    precommit_block.precommit(ctx);
                }

                Ok(false)
            }
        }
    }

    pub fn shutdown(&mut self, has_err: bool) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(has_err);
        }
    }

    pub fn wait_shutdown(&mut self) -> Result<()> {
        let mut exchanges_handler = std::mem::take(&mut self.exchange_handler);
        futures::executor::block_on(async move {
            while let Some(exchange_handler) = exchanges_handler.pop() {
                match exchange_handler.await {
                    Ok(Ok(_)) => Ok(()),
                    Ok(Err(cause)) => Err(cause),
                    Err(join_error) => match join_error.is_cancelled() {
                        true => Err(ErrorCode::TokioError("Tokio error is cancelled.")),
                        false => {
                            let panic_error = join_error.into_panic();
                            match panic_error.downcast_ref::<&'static str>() {
                                None => match panic_error.downcast_ref::<String>() {
                                    None => {
                                        Err(ErrorCode::PanicError("Sorry, unknown panic message"))
                                    }
                                    Some(message) => {
                                        Err(ErrorCode::PanicError(message.to_string()))
                                    }
                                },
                                Some(message) => Err(ErrorCode::PanicError(message.to_string())),
                            }
                        }
                    },
                }?;
            }

            Ok(())
        })
    }
}
