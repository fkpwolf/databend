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

use std::collections::VecDeque;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use common_base::runtime::TrackedFuture;
use common_base::runtime::TrySpawn;
use common_exception::Result;
use petgraph::dot::Config;
use petgraph::dot::Dot;
use petgraph::prelude::EdgeIndex;
use petgraph::prelude::NodeIndex;
use petgraph::prelude::StableGraph;
use petgraph::Direction;
use tracing::debug;
use tracing::info;

use crate::pipelines::executor::executor_condvar::WorkersCondvar;
use crate::pipelines::executor::executor_tasks::ExecutorTasksQueue;
use crate::pipelines::executor::executor_worker_context::ExecutorTask;
use crate::pipelines::executor::executor_worker_context::ExecutorWorkerContext;
use crate::pipelines::executor::processor_async_task::ProcessorAsyncTask;
use crate::pipelines::executor::PipelineExecutor;
use crate::pipelines::pipeline::Pipeline;
use crate::pipelines::processors::connect;
use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::DirectedEdge;
use crate::pipelines::processors::UpdateList;
use crate::pipelines::processors::UpdateTrigger;

enum State {
    Idle,
    // Preparing,
    Processing,
    Finished,
}

struct Node {
    state: std::sync::Mutex<State>,
    processor: ProcessorPtr,

    updated_list: Arc<UpdateList>,
    #[allow(dead_code)]
    inputs_port: Vec<Arc<InputPort>>,
    #[allow(dead_code)]
    outputs_port: Vec<Arc<OutputPort>>,
}

impl Node {
    pub fn create(
        processor: &ProcessorPtr,
        inputs_port: &[Arc<InputPort>],
        outputs_port: &[Arc<OutputPort>],
    ) -> Arc<Node> {
        Arc::new(Node {
            state: std::sync::Mutex::new(State::Idle),
            processor: processor.clone(),
            updated_list: UpdateList::create(),
            inputs_port: inputs_port.to_vec(),
            outputs_port: outputs_port.to_vec(),
        })
    }

    pub unsafe fn trigger(&self, queue: &mut VecDeque<DirectedEdge>) {
        self.updated_list.trigger(queue)
    }

    pub unsafe fn create_trigger(&self, index: EdgeIndex) -> *mut UpdateTrigger {
        self.updated_list.create_trigger(index)
    }
}

struct ExecutingGraph {
    graph: StableGraph<Arc<Node>, ()>,
}

type StateLockGuard = ExecutingGraph;

impl ExecutingGraph {
    pub fn create(mut pipeline: Pipeline) -> Result<ExecutingGraph> {
        let mut graph = StableGraph::new();
        Self::init_graph(&mut pipeline, &mut graph);
        Ok(ExecutingGraph { graph })
    }

    pub fn from_pipelines(mut pipelines: Vec<Pipeline>) -> Result<ExecutingGraph> {
        let mut graph = StableGraph::new();

        for pipeline in &mut pipelines {
            Self::init_graph(pipeline, &mut graph);
        }
        debug!("Create executing graph by from_pipelines:{:?}", graph);
        Ok(ExecutingGraph { graph })
    }

    fn init_graph(pipeline: &mut Pipeline, graph: &mut StableGraph<Arc<Node>, ()>) {
        #[derive(Debug)]
        struct Edge {
            source_port: usize,
            source_node: NodeIndex,
            target_port: usize,
            target_node: NodeIndex,
        }

        let mut pipes_edges: Vec<Vec<Edge>> = Vec::new();
        for pipe in &pipeline.pipes {
            assert_eq!(
                pipe.input_length,
                pipes_edges.last().map(|x| x.len()).unwrap_or_default()
            );

            let mut edge_index = 0;
            let mut pipe_edges = Vec::with_capacity(pipe.output_length);

            for item in &pipe.items {
                let node = Node::create(&item.processor, &item.inputs_port, &item.outputs_port);

                let graph_node_index = graph.add_node(node.clone());
                unsafe {
                    item.processor.set_id(graph_node_index);
                }

                for offset in 0..item.inputs_port.len() {
                    let last_edges = pipes_edges.last_mut().unwrap();

                    last_edges[edge_index].target_port = offset;
                    last_edges[edge_index].target_node = graph_node_index;
                    edge_index += 1;
                }

                for offset in 0..item.outputs_port.len() {
                    pipe_edges.push(Edge {
                        source_port: offset,
                        source_node: graph_node_index,
                        target_port: 0,
                        target_node: Default::default(),
                    });
                }
            }

            pipes_edges.push(pipe_edges);
        }

        // The last pipe cannot contain any output edge.
        assert!(pipes_edges.last().map(|x| x.is_empty()).unwrap_or_default());
        pipes_edges.pop();

        for pipe_edges in &pipes_edges {
            for edge in pipe_edges {
                let edge_index = graph.add_edge(edge.source_node, edge.target_node, ());
                unsafe {
                    let (target_node, target_port) = (edge.target_node, edge.target_port);
                    let input_trigger = graph[target_node].create_trigger(edge_index);
                    graph[target_node].inputs_port[target_port].set_trigger(input_trigger);

                    let (source_node, source_port) = (edge.source_node, edge.source_port);
                    let output_trigger = graph[source_node].create_trigger(edge_index);
                    graph[source_node].outputs_port[source_port].set_trigger(output_trigger);

                    connect(
                        &graph[target_node].inputs_port[target_port],
                        &graph[source_node].outputs_port[source_port],
                    );
                }
            }
        }
    }

    /// # Safety
    ///
    /// Method is thread unsafe and require thread safe call
    pub unsafe fn init_schedule_queue(locker: &StateLockGuard) -> Result<ScheduleQueue> {
        let mut schedule_queue = ScheduleQueue::create();
        for sink_index in locker.graph.externals(Direction::Outgoing) {
            ExecutingGraph::schedule_queue(locker, sink_index, &mut schedule_queue)?;
        }

        Ok(schedule_queue)
    }

    /// # Safety
    ///
    /// Method is thread unsafe and require thread safe call
    #[tracing::instrument(level = "debug", skip_all)]
    pub unsafe fn schedule_queue(
        locker: &StateLockGuard,
        index: NodeIndex,
        schedule_queue: &mut ScheduleQueue,
    ) -> Result<()> {
        debug!("schedule_queue next step of node: {:?}", index);
        let mut need_schedule_nodes = VecDeque::new();
        let mut need_schedule_edges = VecDeque::new();

        need_schedule_nodes.push_back(index); // the index node has completed
        while !need_schedule_nodes.is_empty() || !need_schedule_edges.is_empty() {
            // To avoid lock too many times, we will try to cache lock.
            // graph is shared between threads
            let mut state_guard_cache = None; // start of lock?

            if need_schedule_nodes.is_empty() {
                let edge = need_schedule_edges.pop_front().unwrap();
                let target_index = DirectedEdge::get_target(&edge, &locker.graph)?;

                let node = &locker.graph[target_index];
                let node_state = node.state.lock().unwrap();

                if matches!(*node_state, State::Idle) {
                    state_guard_cache = Some(node_state);
                    debug!(
                        "candidate node: {:?} of edge: {:?}",
                        target_index,
                        edge.print(&locker.graph)
                    );
                    need_schedule_nodes.push_back(target_index);
                }
            }

            if let Some(schedule_index) = need_schedule_nodes.pop_front() {
                let node = &locker.graph[schedule_index];

                if state_guard_cache.is_none() {
                    state_guard_cache = Some(node.state.lock().unwrap());
                }
                let event = node.processor.event()?; // refresh status?
                if tracing::enabled!(tracing::Level::TRACE) {
                    tracing::trace!(
                        "node id: {:?}, name: {:?}, event: {:?}",
                        node.processor.id(),
                        node.processor.name(),
                        event
                    );
                }
                debug!(
                    "node id: {:?}, name: {:?}, event: {:?}",
                    node.processor.id(),
                    node.processor.name(),
                    event
                );
                let processor_state = match event {
                    Event::Finished => State::Finished,
                    Event::NeedData | Event::NeedConsume => State::Idle,
                    Event::Sync => {
                        debug!(
                            "schedule sync node: {:?}, name: {:?}",
                            node.processor.id(),
                            node.processor.name()
                        );
                        schedule_queue.push_sync(node.processor.clone());
                        State::Processing
                    }
                    Event::Async => {
                        debug!(
                            "schedule async node: {:?}, name: {:?}",
                            node.processor.id(),
                            node.processor.name()
                        );
                        schedule_queue.push_async(node.processor.clone());
                        State::Processing
                    }
                };

                let before_list = need_schedule_edges
                    .iter()
                    .map(|x| x.print(&locker.graph))
                    .collect::<Vec<String>>();
                info!("need_schedule_edges before: {:?}", before_list);

                node.trigger(&mut need_schedule_edges); // put candidated edge

                let after_list = need_schedule_edges
                    .iter()
                    .map(|x| x.print(&locker.graph))
                    .collect::<Vec<String>>();
                info!("need_schedule_edges after: {:?}", after_list);

                *state_guard_cache.unwrap() = processor_state;
            }
        }

        Ok(())
    }
}

pub struct ScheduleQueue {
    pub sync_queue: VecDeque<ProcessorPtr>,
    pub async_queue: VecDeque<ProcessorPtr>,
}

impl ScheduleQueue {
    pub fn create() -> ScheduleQueue {
        ScheduleQueue {
            sync_queue: VecDeque::new(),
            async_queue: VecDeque::new(),
        }
    }

    #[inline]
    pub fn push_sync(&mut self, processor: ProcessorPtr) {
        self.sync_queue.push_back(processor);
    }

    #[inline]
    pub fn push_async(&mut self, processor: ProcessorPtr) {
        self.async_queue.push_back(processor);
    }

    pub fn schedule_tail(mut self, global: &ExecutorTasksQueue, ctx: &mut ExecutorWorkerContext) {
        let mut tasks = VecDeque::with_capacity(self.sync_queue.len());

        while let Some(processor) = self.sync_queue.pop_front() {
            tasks.push_back(ExecutorTask::Sync(processor));
        }

        global.push_tasks(ctx, tasks)
    }

    pub fn schedule(
        mut self,
        global: &Arc<ExecutorTasksQueue>,
        context: &mut ExecutorWorkerContext,
        executor: &PipelineExecutor,
    ) {
        debug_assert!(!context.has_task());

        while let Some(processor) = self.async_queue.pop_front() {
            Self::schedule_async_task(
                processor,
                context.query_id.clone(),
                executor,
                context.get_worker_num(),
                context.get_workers_condvar().clone(),
                global.clone(),
            )
        }

        if !self.sync_queue.is_empty() {
            self.schedule_sync(global, context);
        }

        if !self.sync_queue.is_empty() {
            self.schedule_tail(global, context);
        }
    }

    pub fn schedule_async_task(
        proc: ProcessorPtr,
        query_id: Arc<String>,
        executor: &PipelineExecutor,
        wakeup_worker_num: usize,
        workers_condvar: Arc<WorkersCondvar>,
        global_queue: Arc<ExecutorTasksQueue>,
    ) {
        unsafe {
            workers_condvar.inc_active_async_worker();
            let process_future = proc.async_process();
            executor
                .async_runtime
                .spawn(TrackedFuture::create(ProcessorAsyncTask::create(
                    query_id,
                    wakeup_worker_num,
                    proc.clone(),
                    global_queue,
                    workers_condvar,
                    process_future,
                )));
        }
    }

    fn schedule_sync(&mut self, _: &ExecutorTasksQueue, ctx: &mut ExecutorWorkerContext) {
        if let Some(processor) = self.sync_queue.pop_front() {
            ctx.set_task(ExecutorTask::Sync(processor));
        }
    }
}

pub struct RunningGraph(ExecutingGraph);

impl RunningGraph {
    pub fn create(pipeline: Pipeline) -> Result<RunningGraph> {
        let graph_state = ExecutingGraph::create(pipeline)?;
        debug!("Create running graph:{:?}", graph_state);
        Ok(RunningGraph(graph_state))
    }

    pub fn from_pipelines(pipelines: Vec<Pipeline>) -> Result<RunningGraph> {
        let graph_state = ExecutingGraph::from_pipelines(pipelines)?;
        debug!("Create running graph:{:?}", graph_state);
        Ok(RunningGraph(graph_state))
    }

    /// # Safety
    ///
    /// Method is thread unsafe and require thread safe call
    pub unsafe fn init_schedule_queue(&self) -> Result<ScheduleQueue> {
        ExecutingGraph::init_schedule_queue(&self.0)
    }

    /// # Safety
    ///
    /// Method is thread unsafe and require thread safe call
    pub unsafe fn schedule_queue(&self, node_index: NodeIndex) -> Result<ScheduleQueue> {
        let mut schedule_queue = ScheduleQueue::create();
        ExecutingGraph::schedule_queue(&self.0, node_index, &mut schedule_queue)?;
        Ok(schedule_queue)
    }

    pub fn interrupt_running_nodes(&self) {
        unsafe {
            for node_index in self.0.graph.node_indices() {
                self.0.graph[node_index].processor.interrupt();
            }
        }
    }
}

impl Debug for Node {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        unsafe { write!(f, "{}", self.processor.name()) }
    }
}

impl Debug for ExecutingGraph {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "{:?}",
            Dot::with_config(&self.graph, &[Config::EdgeNoLabel])
        )
    }
}

impl Debug for RunningGraph {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        // let graph = self.0.read();
        write!(f, "{:?}", self.0)
    }
}

impl Debug for ScheduleQueue {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        #[derive(Debug)]
        #[allow(dead_code)]
        struct QueueItem {
            id: usize,
            name: String,
        }

        unsafe {
            let mut sync_queue = Vec::with_capacity(self.sync_queue.len());
            let mut async_queue = Vec::with_capacity(self.async_queue.len());

            for item in &self.sync_queue {
                sync_queue.push(QueueItem {
                    id: item.id().index(),
                    name: item.name().to_string(),
                })
            }

            for item in &self.async_queue {
                async_queue.push(QueueItem {
                    id: item.id().index(),
                    name: item.name().to_string(),
                })
            }

            f.debug_struct("ScheduleQueue")
                .field("sync_queue", &sync_queue)
                .field("async_queue", &async_queue)
                .finish()
        }
    }
}
