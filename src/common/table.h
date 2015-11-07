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
#ifndef NTU_CAP_UNICAP_COMMON_TABLE_H_
#define NTU_CAP_UNICAP_COMMON_TABLE_H_

#include <stdint.h>
#include <map>
#include <string>
#include <glog/logging.h>
#include "../gen/JobTracker.h"
#include "../gen/TaskTracker.h"
#include "node_info.h"

namespace ntu {
namespace cap {

class Table {
public:

    Table();

    Table(const std::string& table_name, const Table &base_table);

    Table(const TableProperty& table_property);

    Table(const std::string& table_name, const int64_t shard_num, const KeyPartition partition);

    int64_t set_table_name(const std::string& table_name);

    int64_t set_shard_num(const int64_t shard_num);

    int64_t allocate_shard();

    int64_t replicate_shard();

    TableProperty _table_property;
//   std::string _table_name;
//   int64_t     _shard_num;
//   std::map<int64_t, int64_t> _shard_location;
//   std::map<int64_t, std::vector<int64_t>> _node_info;
//   KeyPartition _partition;
};

}
}

#endif /* UNICAP_COMMON_TABLE_H_ */
