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
use std::sync::Arc;
use std::sync::Barrier;

use common_exception::ErrorCode;
use common_exception::Result;
use common_pipeline_core::processors::connect;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::UpdateList;
use petgraph::prelude::EdgeIndex;
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_input_and_output_port() -> Result<()> {
    fn input_port(input: Arc<InputPort>, barrier: Arc<Barrier>) -> impl Fn() + Send {
        move || {
            barrier.wait();
            unsafe {
                let list = UpdateList::create();
                let update_trigger = list.create_trigger(EdgeIndex::new(123));
                input.set_trigger(update_trigger);
                // for index=0, it will trigger one UpdateList.update_edge
                for index in 0..100 {
                    input.set_need_data();
                    while !input.has_data() {}
                    let data = input.pull_data().unwrap();
                    assert_eq!(data.unwrap_err().message(), index.to_string());
                }

                let mut queue = VecDeque::new();
                list.trigger(&mut queue);
                assert_eq!(1, queue.len());
                input.finish(); // this will trigger another UpdateList.update_edge
                assert_eq!(input.is_finished(), true);
            }
        }
    }

    fn output_port(output: Arc<OutputPort>, barrier: Arc<Barrier>) -> impl Fn() + Send {
        move || {
            barrier.wait();
            for index in 0..100 {
                while !output.can_push() {}
                output.push_data(Err(ErrorCode::Ok(index.to_string())));
            }
        }
    }

    unsafe {
        let input = InputPort::create();
        let output = OutputPort::create();
        let barrier = Arc::new(Barrier::new(2));

        connect(&input, &output);
        let thread_1 = std::thread::spawn(input_port(input, barrier.clone()));
        let thread_2 = std::thread::spawn(output_port(output, barrier));

        thread_1.join().unwrap();
        thread_2.join().unwrap();
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_input_and_output_flags() -> Result<()> {
    unsafe {
        let input = InputPort::create();
        let output = OutputPort::create();

        connect(&input, &output);

        output.finish();
        assert!(input.is_finished());
        input.set_need_data();
        assert!(input.is_finished());
    }

    // assert_eq!(output.can_push());
    // input.set_need_data();
    // assert_eq!(!output.can_push());
    Ok(())
}
