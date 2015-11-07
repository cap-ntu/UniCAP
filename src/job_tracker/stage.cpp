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
#include "stage.h"

namespace ntu {
namespace cap {

Stage::Stage() {
        _function_name = "NULL";
        _src_table  = "NULL";
        _dst_table  = "NULL";
        _dst_cf     = "NULL";
        _size       = 0;
        _stage_id   = 0;
}

int64_t Stage::set_function_name(std::string function_name) {
    _function_name = function_name;
    if (UCPUFunctions::singleton()._cpu_functions_p.find(function_name)
            == UCPUFunctions::singleton()._cpu_functions_p.end()) {
        LOG(FATAL) << "CANNOT FIND FUNCTION "
                   << function_name;
    }
    return 1;
}

int64_t Stage::set_src(std::string src_table, std::vector<std::string> src_cf) {
    _src_table = src_table;
    _src_cf = src_cf;
    if (StorageInfo::singleton()._table_info.find(src_table)
            == StorageInfo::singleton()._table_info.end()) {
        LOG(FATAL) << "CANNOT FIND TABLE "
                   << src_table;
    }
    for (auto i : src_cf) {
        if (StorageInfo::singleton()._cf_info[src_table].find(i)
            == StorageInfo::singleton()._cf_info[src_table].end()) {
            LOG(FATAL) << "CANNOT FIND CF "
                       << i;
        }
    }

    int64_t shard_num = StorageInfo::singleton().
                        _table_info[src_table].
                        _table_property.
                        shard_num;

    _shard_allocation = StorageInfo::singleton().
                        _table_info[src_table].
                        _table_property.
                        node_info;

    _size = src_cf.size() * shard_num;

    for (uint64_t i = 0; i < src_cf.size(); ++i) {
        for (int j = 0; j < shard_num; ++j) {
            _wait_task[j + i * shard_num] = std::make_pair(j, src_cf[i]);
        }
    }

    return 1;
}

int64_t Stage::non_src(int64_t task_num) {
    _src_table = "NULL";
    _size      = task_num;

    for (int i = 0; i < task_num; ++i) {
        _wait_task[i] = std::make_pair(-1, "NULL");
    }

    return 1;
}

int64_t Stage::set_dst(std::string dst_table, std::string dst_cf) {
    _dst_table = dst_table;
    _dst_cf = dst_cf;

    if (StorageInfo::singleton()._table_info.find(dst_table)
            == StorageInfo::singleton()._table_info.end()) {
        LOG(FATAL) << "CANNOT FIND TABLE "
                   << dst_table;
    }

    if (StorageInfo::singleton()._cf_info[dst_table].find(dst_cf)
            == StorageInfo::singleton()._cf_info[dst_table].end()) {
        LOG(FATAL) << "CANNOT FIND CF "
                   << dst_cf;
    }

    return 1;
}

uint64_t Stage::size() {
    return _size;
}

int64_t Stage::fetch_task(int64_t node_id, TaskNode &new_task) {
    _lock.lock();
    if (_wait_task.size() == 0) {
        new_task.__set_status(false);
    } else if (_src_table == "NULL") {
        auto it = _wait_task.begin();
        _executing_task[it->first] = it->second;

        new_task.__set_status(true);
        new_task.__set_function_name(_function_name);
        new_task.__set_stage_id(_stage_id);
        new_task.__set_task_id(it->first);
        new_task.__set_src_table_name("NULL");
        new_task.__set_src_cf_name("NULL");
        new_task.__set_src_shard_id(-1);
        new_task.__set_dst_table_name(_dst_table);
        new_task.__set_dst_cf_name(_dst_cf);
        new_task.__set_dst_shard_id(-1);

        _wait_task.erase(it);
    } else {
        auto it = _wait_task.begin();
        for (; it != _wait_task.end(); ++it) {

            auto locality_ptr = std::find(_shard_allocation[node_id].begin(),
                                        _shard_allocation[node_id].end(),
                                        it->second.first);

            if (locality_ptr != _shard_allocation[node_id].end()) {
                break;
            }
        }

        if (it != _wait_task.end()) {
            new_task.__set_status(true);
            new_task.__set_function_name(_function_name);
            new_task.__set_stage_id(_stage_id);
            new_task.__set_task_id(it->first);
            new_task.__set_src_table_name(_src_table);
            new_task.__set_src_cf_name(it->second.second);
            new_task.__set_src_shard_id(it->second.first);
            new_task.__set_dst_table_name(_dst_table);
            new_task.__set_dst_cf_name(_dst_cf);
            new_task.__set_dst_shard_id(-1);

            _executing_task[it->first] = it->second;
            _wait_task.erase(it);

        } else {
            new_task.__set_status(false);
        }
    }
    _lock.unlock();
    return 1;
}

int64_t Stage::complete_task(int64_t task_id) {
    _lock.lock();
    auto i = _executing_task.find(task_id);
    if (i == _executing_task.end()) {
        LOG(FATAL) << "CANNOT FIND EXECUTING TASK";
    } else {
        _complete_task[i->first] = i->second;
        _executing_task.erase(i);
    }
    _lock.unlock();
    return 1;
}

}
}


