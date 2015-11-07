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

#ifndef NTU_CAP_UNICAP_COMMON_STORAGE_INFO_H_
#define NTU_CAP_UNICAP_COMMON_STORAGE_INFO_H_

#include <map>
#include <string>
#include "table.h"
#include "column_family.h"
#include "../gen/JobTracker.h"
#include "../gen/TaskTracker.h"
#include "../storage/storage_systems.h"

namespace ntu {
namespace cap {

class StorageInfo {
public:

    StorageInfo();

    static StorageInfo& singleton();

    std::unordered_map<std::string, Table> _table_info;
    typedef std::map<std::string, ColumnFamily> CfPool;
    std::unordered_map<std::string, CfPool> _cf_info;

    typedef std::shared_ptr<KVStorage> StoragePtr;
    typedef std::unordered_map<std::string, StoragePtr> CfStorage;
    typedef std::unordered_map<int64_t, CfStorage> ShardPtr;
    typedef std::unordered_map<std::string, ShardPtr> TablePtr;
    //table_name -> shard_id -> cf_name -> ptr
    TablePtr _cf_ptr;
private:

};

}
}

#endif /* UNICAP_COMMON_STORAGE_INFO_H_ */
