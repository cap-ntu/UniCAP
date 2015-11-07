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
#ifndef NTU_CAP_UNICAP_COMMON_COLUMN_FAMILY_H_
#define NTU_CAP_UNICAP_COMMON_COLUMN_FAMILY_H_

#include <stdint.h>
#include <map>
#include <string>
#include "../gen/JobTracker.h"
#include "../gen/TaskTracker.h"
#include "node_info.h"
#include "table.h"

namespace ntu {
namespace cap {

class ColumnFamily {
public:
    ColumnFamily();

    ColumnFamily(const ColumnFamilyProperty& cf_property);

    ColumnFamily(const std::string& cf_name,
                 const StorageType::type storage_type);

    int64_t set_storage_type(const StorageType::type storage_type);

    ColumnFamilyProperty _cf_property;
};

}
}
#endif /* UNICAP_COMMON_COLUMN_FAMILY_H_ */
