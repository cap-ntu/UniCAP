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
#include "column_family.h"

namespace ntu {
namespace cap {

ColumnFamily::ColumnFamily() {
}

ColumnFamily::ColumnFamily(const ColumnFamilyProperty& cf_property) {
    _cf_property = cf_property;
}

ColumnFamily::ColumnFamily(const std::string& cf_name,
                 const StorageType::type storage_type) {
    _cf_property.cf_name = cf_name;
    _cf_property.storage_type = storage_type;
    _cf_property.block_size.clear();
}

int64_t ColumnFamily::set_storage_type(const StorageType::type storage_type) {
    _cf_property.storage_type = storage_type;
    return 1;
}

}
}


