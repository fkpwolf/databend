//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::fmt::Debug;
use std::sync::Arc;

use common_base::base::tokio;
use common_base::base::tokio::sync::mpsc::Receiver;
use common_base::base::tokio::sync::mpsc::Sender;
use common_base::runtime::CatchUnwindFuture;
use common_base::runtime::GlobalIORuntime;
use common_base::runtime::TrySpawn;
use common_compress::CompressAlgorithm;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use common_pipeline_core::Pipeline;
use futures::AsyncRead;
use futures_util::stream::FuturesUnordered;
use futures_util::AsyncReadExt;
use futures_util::StreamExt;

use crate::input_formats::Aligner;
use crate::input_formats::BeyondEndReader;
use crate::input_formats::DeserializeSource;
use crate::input_formats::DeserializeTransformer;
use crate::input_formats::InputContext;
use crate::input_formats::InputPlan;
use crate::input_formats::SplitInfo;
use crate::input_formats::StreamPlan;

pub struct Split<I: InputFormatPipe> {
    pub(crate) info: Arc<SplitInfo>,
    pub(crate) rx: Receiver<Result<I::ReadBatch>>,
}

pub struct StreamingReadBatch {
    pub data: Vec<u8>,
    pub path: String,
    pub is_start: bool,
    pub compression: Option<CompressAlgorithm>,
}

#[async_trait::async_trait]
pub trait AligningStateTrait: Sync + Sized {
    type Pipe: InputFormatPipe<AligningState = Self>;
    fn try_create(ctx: &Arc<InputContext>, split_info: &Arc<SplitInfo>) -> Result<Self>;

    fn align(
        &mut self,
        read_batch: Option<<Self::Pipe as InputFormatPipe>::ReadBatch>,
    ) -> Result<Vec<<Self::Pipe as InputFormatPipe>::RowBatch>>;

    fn read_beyond_end(&self) -> Option<BeyondEndReader> {
        None
    }
}

pub trait BlockBuilderTrait {
    type Pipe: InputFormatPipe<BlockBuilder = Self>;
    fn create(ctx: Arc<InputContext>) -> Self;

    fn deserialize(
        &mut self,
        batch: Option<<Self::Pipe as InputFormatPipe>::RowBatch>,
    ) -> Result<Vec<DataBlock>>;
}

pub trait ReadBatchTrait: From<Vec<u8>> + Send + Debug {
    fn size(&self) -> usize;
}

impl ReadBatchTrait for Vec<u8> {
    fn size(&self) -> usize {
        self.len()
    }
}

pub trait RowBatchTrait: Send {
    fn size(&self) -> usize;
    fn rows(&self) -> usize;
}

#[async_trait::async_trait]
pub trait InputFormatPipe: Sized + Send + 'static {
    type SplitMeta;
    type ReadBatch: ReadBatchTrait;
    type RowBatch: RowBatchTrait;
    type AligningState: AligningStateTrait<Pipe = Self> + Send;
    type BlockBuilder: BlockBuilderTrait<Pipe = Self> + Send;

    fn get_split_meta(split_info: &Arc<SplitInfo>) -> Option<&Self::SplitMeta> {
        split_info
            .format_info
            .as_ref()?
            .as_any()
            .downcast_ref::<Self::SplitMeta>()
    }

    fn execute_stream(ctx: Arc<InputContext>, pipeline: &mut Pipeline) -> Result<()> {
        let mut input = ctx.source.take_receiver()?;

        let (split_tx, split_rx) = async_channel::bounded(ctx.num_prefetch_splits()?);
        Self::build_pipeline_with_aligner(&ctx, split_rx, pipeline)?;

        GlobalIORuntime::instance().spawn(async move {
            let mut sender: Option<Sender<Result<Self::ReadBatch>>> = None;
            while let Some(batch_result) = input.recv().await {
                match batch_result {
                    Ok(batch) => {
                        if batch.is_start {
                            let (data_tx, data_rx) = tokio::sync::mpsc::channel(1);
                            sender = Some(data_tx);
                            let split_info = Arc::new(SplitInfo::from_stream_split(
                                batch.path.clone(),
                                batch.compression,
                            ));
                            split_tx
                                .send(Ok(Split {
                                    info: split_info,
                                    rx: data_rx,
                                }))
                                .await
                                .expect("fail to send split from stream");
                        }
                        if let Some(s) = sender.as_mut() {
                            s.send(Ok(batch.data.into()))
                                .await
                                .expect("fail to send read batch from stream");
                        }
                    }
                    Err(error) => {
                        if let Some(s) = sender.as_mut() {
                            s.send(Err(error.clone()))
                                .await
                                .expect("fail to send error to from stream");
                        }
                    }
                }
            }
        });
        Ok(())
    }

    fn execute_copy_with_aligner(ctx: Arc<InputContext>, pipeline: &mut Pipeline) -> Result<()> {
        let (split_tx, split_rx) = async_channel::bounded(ctx.num_prefetch_splits()?);
        Self::build_pipeline_with_aligner(&ctx, split_rx, pipeline)?;

        let ctx_clone = ctx.clone();
        GlobalIORuntime::instance().spawn(async move {
            tracing::debug!("start copy splits feeder");
            for s in &ctx_clone.splits {
                let (data_tx, data_rx) = tokio::sync::mpsc::channel(ctx.num_prefetch_per_split());
                let split_clone = s.clone();
                let ctx_clone2 = ctx_clone.clone();
                tokio::spawn(async move {
                    if let Err(e) =
                        Self::copy_reader_with_aligner(ctx_clone2, split_clone, data_tx).await
                    {
                        tracing::error!("copy split reader error: {:?}", e);
                    } else {
                        tracing::debug!("copy split reader stopped");
                    }
                });
                if split_tx
                    .send(Ok(Split {
                        info: s.clone(),
                        rx: data_rx,
                    }))
                    .await
                    .is_err()
                {
                    break;
                };
            }
            tracing::debug!("end copy splits feeder");
        });

        Ok(())
    }

    fn execute_copy_aligned(ctx: Arc<InputContext>, pipeline: &mut Pipeline) -> Result<()> {
        let (data_tx, data_rx) = async_channel::bounded(ctx.num_prefetch_splits()?);
        Self::build_pipeline_aligned(&ctx, data_rx, pipeline)?;

        let ctx_clone = ctx.clone();
        let p = 3;
        GlobalIORuntime::instance().spawn(async move {
            for splits in ctx_clone.splits.chunks(p) {
                let ctx_clone2 = ctx_clone.clone();
                let row_batch_tx = data_tx.clone();
                let splits = splits.to_owned().clone();
                tokio::spawn(async move {
                    let mut futs = FuturesUnordered::new();
                    for s in splits.into_iter() {
                        let fut =
                            CatchUnwindFuture::create(Self::read_split(ctx_clone2.clone(), s));
                        futs.push(fut);
                    }
                    while let Some(row_batch) = futs.next().await {
                        match row_batch {
                            Ok(row_batch) => {
                                if row_batch_tx.send(row_batch).await.is_err() {
                                    break;
                                }
                            }
                            Err(cause) => {
                                row_batch_tx.send(Err(cause)).await.ok();
                                break;
                            }
                        }
                    }
                });
            }
        });
        Ok(())
    }

    fn build_pipeline_aligned(
        ctx: &Arc<InputContext>,
        row_batch_rx: async_channel::Receiver<Result<Self::RowBatch>>,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let max_threads = ctx.settings.get_max_threads()? as usize;
        pipeline.add_source(
            |output| DeserializeSource::<Self>::create(ctx.clone(), output, row_batch_rx.clone()),
            max_threads,
        )?;
        Ok(())
    }

    fn build_pipeline_with_aligner(
        ctx: &Arc<InputContext>,
        split_rx: async_channel::Receiver<Result<Split<Self>>>,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let n_threads = ctx.settings.get_max_threads()? as usize;
        let max_aligner = match ctx.plan {
            InputPlan::CopyInto(_) => ctx.splits.len(),
            InputPlan::StreamingLoad(StreamPlan { is_multi_part, .. }) => {
                if is_multi_part {
                    3
                } else {
                    1
                }
            }
        };
        let (row_batch_tx, row_batch_rx) = crossbeam_channel::bounded(n_threads);
        pipeline.add_source(
            |output| {
                Aligner::<Self>::try_create(
                    output,
                    ctx.clone(),
                    split_rx.clone(),
                    row_batch_tx.clone(),
                )
            },
            std::cmp::min(max_aligner, n_threads),
        )?;
        pipeline.resize(n_threads)?;
        pipeline.add_transform(|input, output| {
            DeserializeTransformer::<Self>::create(ctx.clone(), input, output, row_batch_rx.clone())
        })?;
        Ok(())
    }

    async fn read_split(
        _ctx: Arc<InputContext>,
        _split_info: Arc<SplitInfo>,
    ) -> Result<Self::RowBatch> {
        unimplemented!()
    }

    #[tracing::instrument(level = "debug", skip(ctx, batch_tx))]
    async fn copy_reader_with_aligner(
        ctx: Arc<InputContext>,
        split_info: Arc<SplitInfo>,
        batch_tx: Sender<Result<Self::ReadBatch>>,
    ) -> Result<()> {
        tracing::debug!("started");
        let operator = ctx.source.get_operator()?;
        let object = operator.object(&split_info.file.path);
        let offset = split_info.offset as u64;
        let size = split_info.size;
        let mut batch_size = ctx.read_batch_size.min(size);
        let mut reader = object.range_reader(offset..offset + size as u64).await?;
        let mut total_read = 0;
        loop {
            batch_size = batch_size.min(size - total_read);
            let mut batch = vec![0u8; batch_size];
            let n = read_full(&mut reader, &mut batch[0..]).await?;
            if n == 0 {
                if total_read != size {
                    return Err(ErrorCode::BadBytes(format!(
                        "split {} expect {} bytes, read only {} bytes",
                        split_info, size, total_read
                    )));
                }
                break;
            } else {
                total_read += n;
                batch.truncate(n);
                tracing::debug!("read {} bytes", n);
                if let Err(e) = batch_tx.send(Ok(batch.into())).await {
                    tracing::warn!("fail to send ReadBatch: {}", e);
                    break;
                }
            }
        }
        tracing::debug!("finished");
        Ok(())
    }
}

pub async fn read_full<R: AsyncRead + Unpin>(reader: &mut R, buf: &mut [u8]) -> Result<usize> {
    let mut buf = &mut buf[0..];
    let mut n = 0;
    while !buf.is_empty() {
        let read = reader.read(buf).await?;
        if read == 0 {
            break;
        }
        n += read;
        buf = &mut buf[read..]
    }
    Ok(n)
}
