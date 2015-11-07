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
#include "node_info.h"
namespace ntu {
namespace cap {

NodeInfo::NodeInfo() {
    _task_tracker_number = 0;
    _ready_task_tracker_number = 0;
    _node_id = 0;
    _node_num = 0;
    _name_length = 0;
    _port = 0;
    _storage_weight = 1;
    _master_port = 9010;

    time_t rawtime;
    struct tm * timeinfo;
    char buffer[80];
    time (&rawtime);
    timeinfo = localtime(&rawtime);

    strftime(buffer,80,"%d%m%Y%I%M%S", timeinfo);
    _app_name = std::string(buffer);
    _root_dir = "/unicap/";
}

NodeInfo& NodeInfo::singleton() {
    static NodeInfo node_info;
    return node_info;
}

}
}



