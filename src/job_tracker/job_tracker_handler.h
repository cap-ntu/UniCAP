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

#ifndef NTU_CAP_UNICAP_JOB_TRACKER_JOB_TRACKER_HANDLER_H_
#define NTU_CAP_UNICAP_JOB_TRACKER_JOB_TRACKER_HANDLER_H_

#include <stdint.h>
#include <map>
#include <utility>
#include <mutex>
#include <glog/logging.h>
#include "../gen/JobTracker.h"
#include "../common/node_info.h"
#include "../common/storage_info.h"
#include "../computing/cpu_functions.h"
#include "../../app/user_cpu_functions.h"
#include "scheduler.h"

namespace ntu {
namespace cap {

class JobTrackerHandler : virtual public JobTrackerIf {
public:
    JobTrackerHandler();

    int64_t register_task_tracker(const int64_t node_id,
                                  const std::string& node_name,
                                  const int64_t storage_weight);

    void get_all_task_tracker_info(std::map<int64_t,
                                   TaskTrackerInfo> & _return);

    void fetch_cpu_task(TaskNode& _return, const int64_t task_tracker_id);

    void fetch_gpu_task(TaskNode& _return, const int64_t task_tracker_id);

    int64_t complete_cpu_task(const int64_t stage_id, const int64_t task_id);

    int64_t complete_gpu_task(const int64_t stage_id, const int64_t task_id);

private:
    std::mutex _register_lock;
    int64_t _base_port = NodeInfo::singleton()._master_port + 100;
};

}
}
#endif /* UNICAP_JOB_TRACKER_JOB_TRACKER_HANDLER_H_ */
