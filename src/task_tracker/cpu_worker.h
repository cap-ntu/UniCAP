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

#ifndef NTU_CAP_UNICAP_CPU_WORKER_H_
#define NTU_CAP_UNICAP_CPU_WORKER_H_

#include <vector>
#include "../common/unicap_client.h"
#include "../common/storage_info.h"
#include "../tools/include/boost/threadpool.hpp"
#include "../computing/cpu_functions.h"
#include "../../app/user_cpu_functions.h"
#include "cpu_networks.h"

namespace ntu {
namespace cap {

class CPUWorker {

public:

    static CPUWorker& singleton() {
        static CPUWorker client;
        return client;
    }

    CPUWorker();

    CPUWorker(int64_t worker_number);

    static int64_t cpu_execute_tasks(int64_t worker_number);

    static int64_t functions(std::string function_name, TaskNode task);

    std::thread cpu_worker_start();

    static int64_t create_network();

    int64_t _worker_number;

};

}
}

#endif /* NTU_CAP_UNICAP_CPU_WORKER_H_ */
