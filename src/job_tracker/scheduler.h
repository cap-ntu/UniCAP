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

#ifndef NTU_CAP_UNICAP_JOB_TRACKER_SCHEDULER_H_
#define NTU_CAP_UNICAP_JOB_TRACKER_SCHEDULER_H_

#include <map>
#include <string>
#include "stage.h"

namespace ntu {
namespace cap {

class Scheduler {
public:

    static Scheduler& singleton() {
        static Scheduler scheduler;
        return scheduler;
    }

    int64_t push_back(std::shared_ptr<Stage> new_stage) {
        int64_t id = _stage_pool.size();
        new_stage->_stage_id = id;
        _stage_pool.push_back(new_stage);
        _stage_num = _stage_pool.size();
        return id;
    }

    int64_t fetch_cpu_task(TaskNode& _return, const int64_t task_tracker_id) {
        for (int64_t i = 0; i < _stage_num; ++i) {
            if (_stage_pool[i]->_complete_task.size() != _stage_pool[i]->size()) {
                _stage_pool[i]->fetch_task(task_tracker_id, _return);
                break;
            }
        }
        return 1;
    }

    bool check_status(int64_t id) {
        if (_stage_pool[id]->_complete_task.size() != _stage_pool[id]->size()) {
            return false;
        } else {
            return true;
        }
    }

    //stage_id stage
    std::vector<std::shared_ptr<Stage>> _stage_pool;
    int64_t            _stage_num;
};

}
}

#endif /* UNICAP_JOB_TRACKER_SCHEDULER_H_ */
