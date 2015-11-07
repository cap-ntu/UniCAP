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

#include "storage.h"
namespace ntu {
namespace cap {

int64_t Storage::vector_put_int(const std::string& table,
            const std::string& cf,
            const int64_t shard_id,
            const std::vector<std::string>& row_key,
            const std::vector<std::string>& column_key,
            const std::vector<int64_t>& value) {

    int64_t node_id = StorageInfo::singleton().
            _table_info[table].
            _table_property.
            shard_location[shard_id];

    if(CPUNetworks::singleton()._cpu_networks.find(std::this_thread::get_id())
            != CPUNetworks::singleton()._cpu_networks.end()) {
        auto i = CPUNetworks::singleton()._cpu_networks[std::this_thread::get_id()][node_id];
        i->open_transport();
        i->method()->vector_put_int(table,shard_id, cf, row_key, column_key, value);
        i->close_transport();
    } else {
        auto i =  NodeInfo::singleton()._client_task_tracker[node_id];
        i->open_transport();
        i->method()->vector_put_int(table,shard_id, cf, row_key, column_key, value);
        i->close_transport();
    }
    return 1;
}

int64_t Storage::vector_put_double(const std::string& table,
            const std::string& cf,
            const int64_t shard_id,
            const std::vector<std::string>& row_key,
            const std::vector<std::string>& column_key,
            const std::vector<double>& value) {

    int64_t node_id = StorageInfo::singleton().
            _table_info[table].
            _table_property.
            shard_location[shard_id];

    if(CPUNetworks::singleton()._cpu_networks.find(std::this_thread::get_id())
            != CPUNetworks::singleton()._cpu_networks.end()) {
        auto i = CPUNetworks::singleton()._cpu_networks[std::this_thread::get_id()][node_id];
        i->open_transport();
        i->method()->vector_put_double(table,shard_id, cf, row_key, column_key, value);
        i->close_transport();
    } else {
        auto i =  NodeInfo::singleton()._client_task_tracker[node_id];
        i->open_transport();
        i->method()->vector_put_double(table,shard_id, cf, row_key, column_key, value);
        i->close_transport();
    }
    return 1;
}

int64_t Storage::vector_put_string(const std::string& table,
            const std::string& cf,
            const int64_t shard_id,
            const std::vector<std::string>& row_key,
            const std::vector<std::string>& column_key,
            const std::vector<std::string>& value) {

    int64_t node_id = StorageInfo::singleton().
            _table_info[table].
            _table_property.
            shard_location[shard_id];

    if(CPUNetworks::singleton()._cpu_networks.find(std::this_thread::get_id())
            != CPUNetworks::singleton()._cpu_networks.end()) {
        auto i = CPUNetworks::singleton()._cpu_networks[std::this_thread::get_id()][node_id];
        i->open_transport();
        i->method()->vector_put_string(table,shard_id, cf, row_key, column_key, value);
        i->close_transport();
    } else {
        auto i =  NodeInfo::singleton()._client_task_tracker[node_id];
        i->open_transport();
        i->method()->vector_put_string(table,shard_id, cf, row_key, column_key, value);
        i->close_transport();
    }
    return 1;
}


int64_t Storage::timed_vector_put_int(const std::string& table,
            const std::string& cf,
            const int64_t shard_id,
            const std::vector<std::string>& row_key,
            const std::vector<std::string>& column_key,
            const int64_t time_stamp,
            const std::vector<int64_t>& value) {

    int64_t node_id = StorageInfo::singleton().
            _table_info[table].
            _table_property.
            shard_location[shard_id];

    if(CPUNetworks::singleton()._cpu_networks.find(std::this_thread::get_id())
            != CPUNetworks::singleton()._cpu_networks.end()) {
        auto i = CPUNetworks::singleton()._cpu_networks[std::this_thread::get_id()][node_id];
        i->open_transport();
        i->method()->timed_vector_put_int(table,shard_id, cf, row_key, column_key, time_stamp, value);
        i->close_transport();
    } else {
        auto i =  NodeInfo::singleton()._client_task_tracker[node_id];
        i->open_transport();
        i->method()->timed_vector_put_int(table,shard_id, cf, row_key, column_key, time_stamp, value);
        i->close_transport();
    }
    return 1;
}

int64_t Storage::timed_vector_put_double(const std::string& table,
            const std::string& cf,
            const int64_t shard_id,
            const std::vector<std::string>& row_key,
            const std::vector<std::string>& column_key,
            const int64_t time_stamp,
            const std::vector<double>& value) {

    int64_t node_id = StorageInfo::singleton().
            _table_info[table].
            _table_property.
            shard_location[shard_id];

    if(CPUNetworks::singleton()._cpu_networks.find(std::this_thread::get_id())
            != CPUNetworks::singleton()._cpu_networks.end()) {
        auto i = CPUNetworks::singleton()._cpu_networks[std::this_thread::get_id()][node_id];
        i->open_transport();
        i->method()->timed_vector_put_double(table,shard_id, cf, row_key, column_key, time_stamp, value);
        i->close_transport();
    } else {
        auto i =  NodeInfo::singleton()._client_task_tracker[node_id];
        i->open_transport();
        i->method()->timed_vector_put_double(table,shard_id, cf, row_key, column_key, time_stamp, value);
        i->close_transport();
    }
    return 1;
}

int64_t Storage::timed_vector_put_string(const std::string& table,
            const std::string& cf,
            const int64_t shard_id,
            const std::vector<std::string>& row_key,
            const std::vector<std::string>& column_key,
            const int64_t time_stamp,
            const std::vector<std::string>& value) {

    int64_t node_id = StorageInfo::singleton().
            _table_info[table].
            _table_property.
            shard_location[shard_id];

    if(CPUNetworks::singleton()._cpu_networks.find(std::this_thread::get_id())
            != CPUNetworks::singleton()._cpu_networks.end()) {
        auto i = CPUNetworks::singleton()._cpu_networks[std::this_thread::get_id()][node_id];
        i->open_transport();
        i->method()->timed_vector_put_string(table,shard_id, cf, row_key, column_key, time_stamp, value);
        i->close_transport();
    } else {
        auto i =  NodeInfo::singleton()._client_task_tracker[node_id];
        i->open_transport();
        i->method()->timed_vector_put_string(table,shard_id, cf, row_key, column_key, time_stamp, value);
        i->close_transport();
    }
    return 1;
}

int64_t Storage::vector_get_int(const std::string& table,
            const std::string& cf,
            const int64_t shard_id,
            const std::vector<std::string>& row_key,
            const std::vector<std::string>& column_key,
            std::vector<int64_t>& value) {

    value.clear();
    int64_t node_id = StorageInfo::singleton().
            _table_info[table].
            _table_property.
            shard_location[shard_id];

    if(CPUNetworks::singleton()._cpu_networks.find(std::this_thread::get_id())
            != CPUNetworks::singleton()._cpu_networks.end()) {
        auto i = CPUNetworks::singleton()._cpu_networks[std::this_thread::get_id()][node_id];
        i->open_transport();
        i->method()->vector_get_int(value, table, shard_id, cf, row_key, column_key);
        i->close_transport();
    } else {
        auto i =  NodeInfo::singleton()._client_task_tracker[node_id];
        i->open_transport();
        i->method()->vector_get_int(value, table, shard_id, cf, row_key, column_key);
        i->close_transport();
    }
    return 1;
}

int64_t Storage::vector_get_double(const std::string& table,
            const std::string& cf,
            const int64_t shard_id,
            const std::vector<std::string>& row_key,
            const std::vector<std::string>& column_key,
            std::vector<double>& value) {

    value.clear();
    int64_t node_id = StorageInfo::singleton().
            _table_info[table].
            _table_property.
            shard_location[shard_id];

    if(CPUNetworks::singleton()._cpu_networks.find(std::this_thread::get_id())
            != CPUNetworks::singleton()._cpu_networks.end()) {
        auto i = CPUNetworks::singleton()._cpu_networks[std::this_thread::get_id()][node_id];
        i->open_transport();
        i->method()->vector_get_double(value, table, shard_id, cf, row_key, column_key);
        i->close_transport();
    } else {
        auto i =  NodeInfo::singleton()._client_task_tracker[node_id];
        i->open_transport();
        i->method()->vector_get_double(value, table, shard_id, cf, row_key, column_key);
        i->close_transport();
    }
    return 1;
}

int64_t Storage::vector_get_string(const std::string& table,
            const std::string& cf,
            const int64_t shard_id,
            const std::vector<std::string>& row_key,
            const std::vector<std::string>& column_key,
            std::vector<std::string>& value) {

    value.clear();
    int64_t node_id = StorageInfo::singleton().
            _table_info[table].
            _table_property.
            shard_location[shard_id];

    if(CPUNetworks::singleton()._cpu_networks.find(std::this_thread::get_id())
            != CPUNetworks::singleton()._cpu_networks.end()) {
        auto i = CPUNetworks::singleton()._cpu_networks[std::this_thread::get_id()][node_id];
        i->open_transport();
        i->method()->vector_get_string(value, table, shard_id, cf, row_key, column_key);
        i->close_transport();
    } else {
        auto i =  NodeInfo::singleton()._client_task_tracker[node_id];
        i->open_transport();
        i->method()->vector_get_string(value, table, shard_id, cf, row_key, column_key);
        i->close_transport();
    }
    return 1;
}

void Storage::scan_int(const std::string& table,
            const std::string& cf,
            const int64_t shard_id,
            std::map<std::string, std::map<std::string, int64_t>>& value) {
    value.clear();
    int64_t node_id = StorageInfo::singleton().
           _table_info[table].
           _table_property.
           shard_location[shard_id];

    if(CPUNetworks::singleton()._cpu_networks.find(std::this_thread::get_id())
            != CPUNetworks::singleton()._cpu_networks.end()) {
        auto i = CPUNetworks::singleton()._cpu_networks[std::this_thread::get_id()][node_id];
        i->open_transport();
        i->method()->scan_all_int(value, table, shard_id, cf);
        i->close_transport();
    } else {
        auto i =  NodeInfo::singleton()._client_task_tracker[node_id];
        i->open_transport();
        i->method()->scan_all_int(value, table, shard_id, cf);
        i->close_transport();
    }
}


void Storage::scan_double(const std::string& table,
            const std::string& cf,
            const int64_t shard_id,
            std::map<std::string, std::map<std::string, double>>& value) {

    value.clear();
    int64_t node_id = StorageInfo::singleton().
           _table_info[table].
           _table_property.
           shard_location[shard_id];

    if(CPUNetworks::singleton()._cpu_networks.find(std::this_thread::get_id())
            != CPUNetworks::singleton()._cpu_networks.end()) {
        auto i = CPUNetworks::singleton()._cpu_networks[std::this_thread::get_id()][node_id];
        i->open_transport();
        i->method()->scan_all_double(value, table, shard_id, cf);
        i->close_transport();
    } else {
        auto i =  NodeInfo::singleton()._client_task_tracker[node_id];
        i->open_transport();
        i->method()->scan_all_double(value, table, shard_id, cf);
        i->close_transport();
    }
}

void Storage::scan_string(const std::string& table,
        const std::string& cf,
        const int64_t shard_id,
        std::map<std::string, std::map<std::string, std::string>>& value) {

    value.clear();
    int64_t node_id = StorageInfo::singleton().
        _table_info[table].
        _table_property.
        shard_location[shard_id];

    if(CPUNetworks::singleton()._cpu_networks.find(std::this_thread::get_id())
            != CPUNetworks::singleton()._cpu_networks.end()) {
        auto i = CPUNetworks::singleton()._cpu_networks[std::this_thread::get_id()][node_id];
        i->open_transport();
        i->method()->scan_all_string(value, table, shard_id, cf);
        i->close_transport();
    } else {
        auto i =  NodeInfo::singleton()._client_task_tracker[node_id];
        i->open_transport();
        i->method()->scan_all_string(value, table, shard_id, cf);
        i->close_transport();
    }
}

void Storage::timed_scan_int(const std::string& table,
        const std::string& cf,
        const int64_t shard_id,
        const int64_t time_stamp,
        std::map<std::string, std::map<std::string, int64_t>>& value) {

    value.clear();
    int64_t node_id = StorageInfo::singleton().
        _table_info[table].
        _table_property.
        shard_location[shard_id];

    if(CPUNetworks::singleton()._cpu_networks.find(std::this_thread::get_id())
            != CPUNetworks::singleton()._cpu_networks.end()) {
        auto i = CPUNetworks::singleton()._cpu_networks[std::this_thread::get_id()][node_id];
        i->open_transport();
        i->method()->timed_scan_int(value, table, shard_id, cf, time_stamp);
        i->close_transport();
    } else {
        auto i =  NodeInfo::singleton()._client_task_tracker[node_id];
        i->open_transport();
        i->method()->timed_scan_int(value, table, shard_id, cf, time_stamp);
        i->close_transport();
    }
}

void Storage::timed_scan_double(const std::string& table,
        const std::string& cf,
        const int64_t shard_id,
        const int64_t time_stamp,
        std::map<std::string, std::map<std::string, double>>& value) {

    value.clear();
    int64_t node_id = StorageInfo::singleton().
        _table_info[table].
        _table_property.
        shard_location[shard_id];

    if(CPUNetworks::singleton()._cpu_networks.find(std::this_thread::get_id())
            != CPUNetworks::singleton()._cpu_networks.end()) {
        auto i = CPUNetworks::singleton()._cpu_networks[std::this_thread::get_id()][node_id];
        i->open_transport();
        i->method()->timed_scan_double(value, table, shard_id, cf, time_stamp);
        i->close_transport();
    } else {
        auto i =  NodeInfo::singleton()._client_task_tracker[node_id];
        i->open_transport();
        i->method()->timed_scan_double(value, table, shard_id, cf, time_stamp);
        i->close_transport();
    }
}

void Storage::timed_scan_string(const std::string& table,
        const std::string& cf,
        const int64_t shard_id,
        const int64_t time_stamp,
        std::map<std::string, std::map<std::string, std::string>>& value) {

    value.clear();
    int64_t node_id = StorageInfo::singleton().
        _table_info[table].
        _table_property.
        shard_location[shard_id];

    if(CPUNetworks::singleton()._cpu_networks.find(std::this_thread::get_id())
            != CPUNetworks::singleton()._cpu_networks.end()) {
        auto i = CPUNetworks::singleton()._cpu_networks[std::this_thread::get_id()][node_id];
        i->open_transport();
        i->method()->timed_scan_string(value, table, shard_id, cf, time_stamp);
        i->close_transport();
    } else {
        auto i =  NodeInfo::singleton()._client_task_tracker[node_id];
        i->open_transport();
        i->method()->timed_scan_string(value, table, shard_id, cf, time_stamp);
        i->close_transport();
    }
}


int64_t Storage::get_shard_num(const std::string& table) {
    return StorageInfo::singleton()._table_info[table]._table_property.shard_num;
}

KeyPartition Storage::get_table_partition(const std::string& table) {
    return StorageInfo::singleton()._table_info[table]._table_property.key_partition;
}

std::shared_ptr<KVStorage> Storage::storage(const std::string& table,
        const std::string& cf,
        const int64_t shard_id) {
    return StorageInfo::singleton()._cf_ptr[table][shard_id][cf];
}


}
}


