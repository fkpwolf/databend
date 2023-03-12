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
use std::env;
use std::sync::Arc;
use std::time::Instant;

use common_base::base::tokio;
use common_base::base::tokio::sync::Notify;
use common_base::runtime::catch_unwind;
use common_base::runtime::GlobalIORuntime;
use common_base::runtime::Runtime;
use common_base::runtime::Thread;
use common_base::runtime::ThreadJoinHandle;
use common_base::runtime::TrySpawn;
use common_exception::ErrorCode;
use common_exception::Result;
use futures::future::select;
use futures_util::future::Either;
use parking_lot::Mutex;
use tracing::info;
use uuid::Uuid;

use crate::pipelines::executor::executor_condvar::WorkersCondvar;
use crate::pipelines::executor::executor_graph::RunningGraph;
use crate::pipelines::executor::executor_graph::ScheduleQueue;
use crate::pipelines::executor::executor_tasks::ExecutorTasksQueue;
use crate::pipelines::executor::executor_worker_context::ExecutorWorkerContext;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::pipeline::Pipeline;

pub type InitCallback = Box<dyn FnOnce() -> Result<()> + Send + Sync + 'static>;

pub type FinishedCallback =
    Box<dyn FnOnce(&Option<ErrorCode>) -> Result<()> + Send + Sync + 'static>;

pub struct PipelineExecutor {
    threads_num: usize,
    graph: RunningGraph,
    workers_condvar: Arc<WorkersCondvar>,
    pub async_runtime: Arc<Runtime>,
    pub global_tasks_queue: Arc<ExecutorTasksQueue>,
    on_init_callback: Mutex<Option<InitCallback>>,
    on_finished_callback: Mutex<Option<FinishedCallback>>,
    settings: ExecutorSettings,
    finished_notify: Arc<Notify>,
    finished_error: Mutex<Option<ErrorCode>>,
    id: Uuid,
}

impl PipelineExecutor {
    pub fn create(
        mut pipeline: Pipeline,
        settings: ExecutorSettings,
    ) -> Result<Arc<PipelineExecutor>> {
        let threads_num = pipeline.get_max_threads();
        let on_init_callback = pipeline.take_on_init();
        let on_finished_callback = pipeline.take_on_finished();

        assert_ne!(threads_num, 0, "Pipeline max threads cannot equals zero.");
        Self::try_create(
            RunningGraph::create(pipeline)?,
            threads_num,
            Mutex::new(Some(on_init_callback)),
            Mutex::new(Some(on_finished_callback)),
            settings,
        )
    }

    #[tracing::instrument(level = "debug", skip_all, name = "PipelineExecutor.from_pipelines")]
    pub fn from_pipelines(
        mut pipelines: Vec<Pipeline>,
        settings: ExecutorSettings,
    ) -> Result<Arc<PipelineExecutor>> {
        if pipelines.is_empty() {
            return Err(ErrorCode::Internal("Executor Pipelines is empty."));
        }

        let threads_num = pipelines
            .iter()
            .map(|x| x.get_max_threads())
            .max()
            .unwrap_or(0);

        let on_init_callback = {
            let pipelines_callback = pipelines
                .iter_mut()
                .map(|x| x.take_on_init())
                .collect::<Vec<_>>();

            pipelines_callback.into_iter().reduce(|left, right| {
                Box::new(move || {
                    left()?;
                    right()
                })
            })
        };

        let on_finished_callback = {
            let pipelines_callback = pipelines
                .iter_mut()
                .map(|x| x.take_on_finished())
                .collect::<Vec<_>>();

            pipelines_callback.into_iter().reduce(|left, right| {
                Box::new(move |arg| {
                    left(arg)?;
                    right(arg)
                })
            })
        };

        assert_ne!(threads_num, 0, "Pipeline max threads cannot equals zero.");
        Self::try_create(
            RunningGraph::from_pipelines(pipelines)?,
            threads_num,
            Mutex::new(on_init_callback),
            Mutex::new(on_finished_callback),
            settings,
        )
    }

    fn try_create(
        graph: RunningGraph,
        threads_num: usize,
        on_init_callback: Mutex<Option<InitCallback>>,
        on_finished_callback: Mutex<Option<FinishedCallback>>,
        settings: ExecutorSettings,
    ) -> Result<Arc<PipelineExecutor>> {
        let workers_condvar = WorkersCondvar::create(threads_num);
        // the global is just node level rather than cluster level?
        let global_tasks_queue = ExecutorTasksQueue::create(threads_num);

        Ok(Arc::new(PipelineExecutor {
            graph,
            threads_num,
            workers_condvar,
            global_tasks_queue,
            on_init_callback,
            on_finished_callback,
            async_runtime: GlobalIORuntime::instance(),
            settings,
            finished_error: Mutex::new(None),
            id: Uuid::new_v4(),
            finished_notify: Arc::new(Notify::new()),
        }))
    }

    fn on_finished(&self, error: &Option<ErrorCode>) -> Result<()> {
        let mut guard = self.on_finished_callback.lock();
        if let Some(on_finished_callback) = guard.take() {
            drop(guard);
            (on_finished_callback)(error)?;
        }

        Ok(())
    }

    pub fn finish(&self, cause: Option<ErrorCode>) {
        if let Some(cause) = cause {
            let mut finished_error = self.finished_error.lock();

            // We only save the cause of the first error.
            if finished_error.is_none() {
                *finished_error = Some(cause);
            }
        }

        self.global_tasks_queue.finish(self.workers_condvar.clone());
        self.graph.interrupt_running_nodes();
        self.finished_notify.notify_waiters();
    }

    pub fn is_finished(&self) -> bool {
        self.global_tasks_queue.is_finished()
    }

    #[tracing::instrument(level = "debug", skip_all, name = "PipelineExecutor.execute")]
    pub fn execute(self: &Arc<Self>) -> Result<()> {
        self.init()?;

        let node_name = env::var("NODE_NAME").unwrap_or_else(|_| "".to_string());
        info!("node name:{}, self:{:?}", node_name, self.id);

        self.start_executor_daemon()?;

        let mut thread_join_handles = self.execute_threads(self.threads_num);

        while let Some(join_handle) = thread_join_handles.pop() {
            let thread_res = join_handle.join().flatten();

            {
                let finished_error_guard = self.finished_error.lock();
                if let Some(error) = finished_error_guard.as_ref() {
                    let may_error = Some(error.clone());
                    drop(finished_error_guard);

                    self.on_finished(&may_error)?;
                    return Err(may_error.unwrap());
                }
            }

            // We will ignore the abort query error, because returned by finished_error if abort query.
            if matches!(&thread_res, Err(error) if error.code() != ErrorCode::ABORTED_QUERY) {
                let may_error = Some(thread_res.unwrap_err());
                self.on_finished(&may_error)?;
                return Err(may_error.unwrap());
            }
        }

        self.on_finished(&None)?;
        Ok(())
    }

    fn init(self: &Arc<Self>) -> Result<()> {
        unsafe {
            // TODO: the on init callback cannot be killed.
            {
                let instant = Instant::now();
                let mut guard = self.on_init_callback.lock();
                if let Some(callback) = guard.take() {
                    drop(guard);
                    if let Err(cause) = callback() {
                        return Err(cause.add_message_back("(while in query pipeline init)"));
                    }
                }

                info!(
                    "Init pipeline successfully, elapsed: {:?}",
                    instant.elapsed()
                );
            }

            let mut init_schedule_queue = self.graph.init_schedule_queue(self.threads_num)?;

            let mut wakeup_worker_id = 0;
            while let Some(proc) = init_schedule_queue.async_queue.pop_front() {
                ScheduleQueue::schedule_async_task(
                    proc.clone(),
                    self.settings.query_id.clone(),
                    self,
                    wakeup_worker_id,
                    self.workers_condvar.clone(),
                    self.global_tasks_queue.clone(),
                );
                wakeup_worker_id += 1;

                if wakeup_worker_id == self.threads_num {
                    wakeup_worker_id = 0;
                }
            }

            let sync_queue = std::mem::take(&mut init_schedule_queue.sync_queue);
            self.global_tasks_queue.init_sync_tasks(sync_queue);

            Ok(())
        }
    }

    fn start_executor_daemon(self: &Arc<Self>) -> Result<()> {
        if !self.settings.max_execute_time.is_zero() {
            // NOTE(wake ref): When runtime scheduling is blocked, holding executor strong ref may cause the executor can not stop.
            let this = Arc::downgrade(self);
            let max_execute_time = self.settings.max_execute_time;
            let finished_notify = self.finished_notify.clone();
            self.async_runtime.spawn(async move {
                let finished_future = Box::pin(finished_notify.notified());
                let max_execute_future = Box::pin(tokio::time::sleep(max_execute_time));
                if let Either::Left(_) = select(max_execute_future, finished_future).await {
                    if let Some(executor) = this.upgrade() {
                        executor.finish(Some(ErrorCode::AbortedQuery(
                            "Aborted query, because the execution time exceeds the maximum execution time limit",
                        )));
                    }
                }
            });
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all, name = "PipelineExecutor.execute_threads")]
    fn execute_threads(self: &Arc<Self>, threads: usize) -> Vec<ThreadJoinHandle<Result<()>>> {
        let node_name = env::var("NODE_NAME").unwrap_or_else(|_| "".to_string());
        info!(
            "execute_threads, thread count: {} on node: {}",
            threads, node_name
        );
        let mut thread_join_handles = Vec::with_capacity(threads);

        for thread_num in 0..threads {
            let this = self.clone();
            #[allow(unused_mut)]
            let mut name = Some(format!("PipelineExecutor-{}-{}", thread_num, node_name));

            #[cfg(debug_assertions)]
            {
                // We need to pass the thread name in the unit test, because the thread name is the test name
                if matches!(std::env::var("UNIT_TEST"), Ok(var_value) if var_value == "TRUE") {
                    if let Some(cur_thread_name) = std::thread::current().name() {
                        name = Some(cur_thread_name.to_string());
                    }
                }
            }

            thread_join_handles.push(Thread::named_spawn(name, move || unsafe {
                let this_clone = this.clone(); // clone on Arc is shadow clone
                let try_result = catch_unwind(move || -> Result<()> {
                    match this_clone.execute_single_thread(thread_num) {
                        Ok(_) => Ok(()),
                        Err(cause) => {
                            if tracing::enabled!(tracing::Level::TRACE) {
                                Err(cause.add_message_back(format!(
                                    " (while in processor thread {})",
                                    thread_num
                                )))
                            } else {
                                Err(cause)
                            }
                        }
                    }
                });

                // finish the pipeline executor when has error or panic
                if let Err(cause) = try_result.flatten() {
                    this.finish(Some(cause));
                }

                Ok(())
            }));
        }
        thread_join_handles
    }

    /// # Safety
    ///
    /// Method is thread unsafe and require thread safe call
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "PipelineExecutor.execute_single_thread"
    )]
    pub unsafe fn execute_single_thread(&self, thread_num: usize) -> Result<()> {
        let node_name = env::var("NODE_NAME").unwrap_or_else(|_| "".to_string());
        info!("node name:{}", node_name);

        let workers_condvar = self.workers_condvar.clone();
        let mut context = ExecutorWorkerContext::create(
            thread_num,
            workers_condvar,
            self.settings.query_id.clone(),
        );

        while !self.global_tasks_queue.is_finished() {
            // When there are not enough tasks, the thread will be blocked, so we need loop check.
            // otherwise the thread will be closed? not reused?
            while !self.global_tasks_queue.is_finished() && !context.has_task() {
                self.global_tasks_queue.steal_task_to_context(&mut context); // steal to current context
            }

            while !self.global_tasks_queue.is_finished() && context.has_task() {
                if let Some(executed_pid) = context.execute_task()? {
                    // Not scheduled graph if pipeline is finished.
                    if !self.global_tasks_queue.is_finished() {
                        // We immediately schedule the processor again.
                        let schedule_queue = self.graph.schedule_queue(executed_pid)?;
                        info!(
                            "after executed {:?}, the queue now is: {:?}, graph {:p}",
                            executed_pid, // index in execute graph
                            schedule_queue,
                            &self.graph // graph is shared between threads
                        );
                        // context has a self queue. why not put all task to global queue so tasks are more evenly across all threads on same node
                        schedule_queue.schedule(&self.global_tasks_queue, &mut context, self);
                    }
                }
            }
        }

        Ok(())
    }

    pub fn format_graph_nodes(&self) -> String {
        self.graph.format_graph_nodes()
    }
}

impl Drop for PipelineExecutor {
    fn drop(&mut self) {
        self.finish(None);
    }
}
