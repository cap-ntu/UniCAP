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

#ifndef UNICAP_COMMON_STORAGE_H_
#define UNICAP_COMMON_STORAGE_H_

#include "table.h"
#include "node_info.h"
#include "storage_info.h"
#include "../gen/JobTracker.h"
#include "../gen/TaskTracker.h"
#include "../task_tracker/cpu_networks.h"

namespace ntu {
namespace cap {

class Storage {
public:
    static int64_t vector_put_int(const std::string& table,
            const std::string& cf,
            const int64_t shard_id,
            const std::vector<std::string>& row_key,
            const std::vector<std::string>& column_key,
            const std::vector<int64_t>& value);

    static int64_t vector_put_double(const std::string& table,
            const std::string& cf,
            const int64_t shard_id,
            const std::vector<std::string>& row_key,
            const std::vector<std::string>& column_key,
            const std::vector<double>& value);

    static int64_t vector_put_string(const std::string& table,
            const std::string& cf,
            const int64_t shard_id,
            const std::vector<std::string>& row_key,
            const std::vector<std::string>& column_key,
            const std::vector<std::string>& value);

    static int64_t timed_vector_put_int(const std::string& table,
            const std::string& cf,
            const int64_t shard_id,
            const std::vector<std::string>& row_key,
            const std::vector<std::string>& column_key,
            const int64_t time_stamp,
            const std::vector<int64_t>& value);

    static int64_t timed_vector_put_double(const std::string& table,
            const std::string& cf,
            const int64_t shard_id,
            const std::vector<std::string>& row_key,
            const std::vector<std::string>& column_key,
            const int64_t time_stamp,
            const std::vector<double>& value);

    static int64_t timed_vector_put_string(const std::string& table,
            const std::string& cf,
            const int64_t shard_id,
            const std::vector<std::string>& row_key,
            const std::vector<std::string>& column_key,
            const int64_t time_stamp,
            const std::vector<std::string>& value);

    static int64_t vector_get_int(const std::string& table,
            const std::string& cf,
            const int64_t shard_id,
            const std::vector<std::string>& row_key,
            const std::vector<std::string>& column_key,
            std::vector<int64_t>& value);

    static int64_t vector_get_double(const std::string& table,
            const std::string& cf,
            const int64_t shard_id,
            const std::vector<std::string>& row_key,
            const std::vector<std::string>& column_key,
            std::vector<double>& value);

    static int64_t vector_get_string(const std::string& table,
            const std::string& cf,
            const int64_t shard_id,
            const std::vector<std::string>& row_key,
            const std::vector<std::string>& column_key,
            std::vector<std::string>& value);

    static void scan_int(const std::string& table,
            const std::string& cf,
            const int64_t shard_id,
            std::map<std::string, std::map<std::string, int64_t>>& value);

    static void scan_double(const std::string& table,
            const std::string& cf,
            const int64_t shard_id,
            std::map<std::string, std::map<std::string, double>>& value);

    static void scan_string(const std::string& table,
            const std::string& cf,
            const  int64_t shard_id,
            std::map<std::string, std::map<std::string, std::string>>& value);

    static void timed_scan_int(const std::string& table,
            const std::string& cf,
            const int64_t shard_id,
            const int64_t time_stamp,
            std::map<std::string, std::map<std::string, int64_t>>& value);

    static void timed_scan_double(const std::string& table,
            const std::string& cf,
            const int64_t shard_id,
            const int64_t time_stamp,
            std::map<std::string, std::map<std::string, double>>& value);

    static void timed_scan_string(const std::string& table,
            const std::string& cf,
            const int64_t shard_id,
            const int64_t time_stamp,
            std::map<std::string, std::map<std::string, std::string>>& value);

    static int64_t get_shard_num(const std::string& table);

    static KeyPartition get_table_partition(const std::string& table);

    static std::shared_ptr<KVStorage> storage(const std::string& table,
            const std::string& cf,
            const int64_t shard_id);
};


}
}
#endif /* UNICAP_COMMON_STORAGE_H_ */
