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
#ifndef NTU_CAP_UNICAP_COMPUTING_CPU_FUNCTIONS_H_
#define NTU_CAP_UNICAP_COMPUTING_CPU_FUNCTIONS_H_

#include <stdint.h>
#include <map>
#include <vector>
#include <string>
#include <chrono>
#include <thread>
#include "../gen/JobTracker.h"
#include "../gen/TaskTracker.h"
#include "../common/storage_info.h"
#include "../common/storage.h"
#include "../common/intermediate_result.h"
#include "../task_tracker/cpu_networks.h"
#include "../tools/include/hdfs/hdfs.h"

namespace ntu {
namespace cap {

typedef int64_t (*cpu_function_p)(TaskNode);

class CPUFunctions {
public:

    static CPUFunctions& singleton() {
       static CPUFunctions cpu_function;
       return cpu_function;
    }

    CPUFunctions();

    static int64_t test (TaskNode new_task);

    static int64_t load_hdfs (TaskNode new_task);

    static int64_t save_hdfs (TaskNode new_task);

    static int64_t load_distributed_cache (TaskNode new_task);

    static int64_t load_hdfs_image(TaskNode new_task);

    std::map<std::string, cpu_function_p> _cpu_functions_p;
};

}
}

#endif /* NTU_CAPUNICAP_COMPUTING_CPU_FUNCTIONS_H_ */
