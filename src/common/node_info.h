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

#ifndef NTU_CAP_UNICAP_JOB_TRACKER_NODE_INFO_H_
#define NTU_CAP_UNICAP_JOB_TRACKER_NODE_INFO_H_

#include <map>
#include <unordered_map>
#include <string>
#include <vector>
#include <utility>
#include <iomanip>
#include <ctime>
#include "../gen/JobTracker.h"
#include "../gen/TaskTracker.h"
#include "unicap_client.h"

namespace ntu {
namespace cap {

class NodeInfo {
public:
    NodeInfo();

    static NodeInfo& singleton();

    //map<node_id, pair<hostname, port>>
    std::map<int64_t, TaskTrackerInfo> _task_tracker_info;
    //number of task trackers in each physical node
    std::map<std::string, int64_t> _physical_node_info;

    int64_t _task_tracker_number;
    int64_t _ready_task_tracker_number;
    int     _node_id;
    int     _node_num;
    int     _name_length;
    int64_t _port;
    int64_t _storage_weight;
    char _processor_name[256];
    std::string _host_name;
    std::string _app_name;
    std::string _root_dir;
    std::string _master_host_name;
    std::string _hdfs_namenode;
    int64_t     _master_port;
    int64_t     _hdfs_namenode_port;
    std::vector<int64_t> _storage_weight_pool;

    typedef boost::shared_ptr<UnicapClient<JobTrackerClient>> ClientToJobPtr;
    ClientToJobPtr _client_job_tracker;

    typedef  boost::shared_ptr<UnicapClient<TaskTrackerClient>> ClientToTaskPtr;
    std::unordered_map<int64_t, ClientToTaskPtr> _client_task_tracker;
};

}
}
#endif /* UNICAP_JOB_TRACKER_NODE_INFO_H_ */
