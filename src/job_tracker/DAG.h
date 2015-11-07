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
#ifndef NTU_CAP_UNICAP_JOB_TRACKER_DAG_H_
#define NTU_CAP_UNICAP_JOB_TRACKER_DAG_H_
#include <math.h>
#include <iostream>
#include <chrono>
#include <thread>
#include <sstream>
#include <string>
#include <fstream>
#include <glog/logging.h>
#include <boost/filesystem.hpp>

#include "../tools/include/yaml-cpp/yaml.h"
#include "job_tracker_server.h"
#include "scheduler.h"
#include "../common/storage.h"
#include "../common/table.h"
#include "../common/column_family.h"
#include "../common/storage_info.h"
#include "../tools/include/hdfs/hdfs.h"

namespace ntu {
namespace cap {

class DAG {

public:
    static std::thread initial();

    static std::thread start_job_tracker(int64_t thread_num = 10);

    static int64_t create_table(const std::string& table_name,
                const int64_t shard_num);

    static int64_t create_table(const std::string& table_name,
                const int64_t shard_num,
                const KeyPartition& partition);

    static int64_t create_table(const std::string& table_name,
                const Table& base_table);

    static int64_t create_cf(const std::string& table_name,
                const std::string& cf_name,
                const StorageType::type cf_type,
                const ValueType::type value_type,
                const std::pair<int64_t, int64_t> size = std::make_pair(1, 1));

    static int64_t create_distributed_cache(const std::string& table_name,
                const std::string& cf_name,
                const std::string& cached_table_name,
                const std::string& cached_cf_name,
                StorageType::type cf_type = StorageType::type::InMemoryKeyValue);

    static int64_t load_local_file(const std::string& path,
                const std::string& table_name,
                const std::string& cf_name,
                const int64_t block_size = 1024*1024*64,
                StorageType::type storage_type = StorageType::type::InMemoryKeyValue);

    static int64_t load_local_img(const std::string& path,
                const std::string& table_name,
                const std::string& cf_name,
                const int64_t block_size = 1024*1024*64);


    static int64_t load_hdfs_file(const std::string& path,
                const std::string& table_name,
                const std::string& cf_name,
                const int64_t block_size = 1024*1024*64,
                StorageType::type storage_type = StorageType::type::InMemoryKeyValue);

    static int64_t load_hdfs_img(const std::string& path,
                const std::string& table_name,
                const std::string& cf_name,
                const int64_t block_size = 1024*1024*64);

    static int64_t save_to_hdfs(const std::string& table_name,
                const std::string& cf_name);

};
}
}

#endif /* UNICAP_JOB_TRACKER_JOB_TRACKER_FUNCTION_H_ */
