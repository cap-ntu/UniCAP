/*
 *Copyright 2015 NTU (http://www.ntu.edu.sg/)
 *Licensed under the Apache License, Version 2.0 (the "License");
 *you may not use this file except in compliance with the License.
 *You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *Unless required by applicable law or agreed to in writing, software
 *distributed under the License is distributed on an "AS IS" BASIS,
 *WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *See the License for the specific language governing permissions and
 *limitations under the License.
*/
#include "cpu_worker.h"

namespace ntu {
namespace cap {

CPUWorker::CPUWorker() {
    _worker_number = 1;
}

CPUWorker::CPUWorker(int64_t worker_number) {
        _worker_number = worker_number;
}

int64_t CPUWorker::create_network() {

    std::thread::id id = std::this_thread::get_id();

    if ( CPUNetworks::singleton().check_thread(id)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        return 1;
    }
    std::unordered_map<int64_t,
        boost::shared_ptr<UnicapClient<TaskTrackerClient>>> cpu_network;
    for (auto& kvp : NodeInfo::singleton()._task_tracker_info) {
        cpu_network[kvp.first] =
           boost::shared_ptr<UnicapClient<TaskTrackerClient>>
               (new UnicapClient<TaskTrackerClient>(kvp.second.host_name,
                                               kvp.second.port));
    }

    boost::shared_ptr<UnicapClient<JobTrackerClient>> job_tracker_network =
        boost::shared_ptr<UnicapClient<JobTrackerClient>>
            (new UnicapClient<JobTrackerClient>(NodeInfo::singleton()._master_host_name,
                                               NodeInfo::singleton()._master_port));


    CPUNetworks::singleton().create_network(id, cpu_network, job_tracker_network);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    DLOG(INFO) << "CREATE STORAGE NETWORK FOR THREAD " << id;

    return 1;
}

int64_t CPUWorker::cpu_execute_tasks(int64_t worker_number) {

    TaskNode new_task;
    std::string function_name;
    boost::threadpool::pool task_pool(worker_number);
    int try_num = 0;

    while (CPUNetworks::singleton().size() != worker_number) {
        for (int i = 0; i < worker_number*1.2; ++i) {
            task_pool.schedule(create_network);
        }
        task_pool.wait();
        ++try_num;
        if (try_num > 5) {
            LOG(FATAL) << "CANNOT CREATE ALL STOAGE NETWORK";
        }
    }

    while(true) {

        if(task_pool.active() == task_pool.size()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            continue;
        }

        NodeInfo::singleton()._client_job_tracker->open_transport();
        NodeInfo::singleton()._client_job_tracker ->
                method() ->fetch_cpu_task(new_task, NodeInfo::singleton()._node_id);
        NodeInfo::singleton()._client_job_tracker ->
                close_transport();

        if (new_task.status) {
            function_name = new_task.function_name;
            auto i = std::bind(CPUWorker::functions, function_name, new_task);
            task_pool.schedule(i);

        } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
    return 1;
}

int64_t CPUWorker::functions(std::string function_name, TaskNode task) {
    DLOG(INFO) << "START TASK "
               << "STAGE: "
               << task.stage_id
               << " ID: "
               << task.task_id;
    UCPUFunctions::singleton()._cpu_functions_p[function_name](task);
    std::thread::id thread_id = std::this_thread::get_id();

    CPUNetworks::singleton()._job_tracker_network[thread_id]->open_transport();
    CPUNetworks::singleton()._job_tracker_network[thread_id]->
                            method()->
                            complete_cpu_task(task.stage_id, task.task_id);
    CPUNetworks::singleton()._job_tracker_network[thread_id]->close_transport();
    DLOG(INFO) << "COMPLETE TASK "
               << "STAGE: "
               << task.stage_id
               << " ID: "
               << task.task_id;
    return 1;
}

std::thread CPUWorker::cpu_worker_start() {
    auto work_thread = std::thread(cpu_execute_tasks, _worker_number);
    return work_thread;
}

}
}


