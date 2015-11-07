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

#ifdef IN_MEMORY_KEY_VALUE

#include <unordered_map>
#include <map>
#include "./kv_base.h"

namespace ntu {
namespace cap {

template<class VALUE_T>
InMemoryKeyValue<VALUE_T>::InMemoryKeyValue():KVStorage() {
}

template<class VALUE_T>
InMemoryKeyValue<VALUE_T>::InMemoryKeyValue(const std::string table_name,
        const int64_t shard_id,
        const std::string cf_name):
        KVStorage(table_name, shard_id, cf_name) {
}

template<class VALUE_T>
InMemoryKeyValue<VALUE_T>::~InMemoryKeyValue() {
}

template<class VALUE_T>
int64_t InMemoryKeyValue<VALUE_T>::vector_put(std::vector<std::string> row_key,
        std::vector<std::string> column_key,
        std::vector<VALUE_T> value) {

    CHECK_EQ(row_key.size(), column_key.size());
    CHECK_EQ(row_key.size(), value.size());
    write_lock _lock(KVStorage::_rwmutex);

    std::string single_key;

    for (uint64_t i = 0; i < row_key.size(); ++i) {
        single_key.clear();
        single_key.append(row_key[i]);
        single_key.append("!");
        single_key.append(column_key[i]);
        _storage_container[single_key] = value[i];
    }
    return 1;
}

template<class VALUE_T>
int64_t InMemoryKeyValue<VALUE_T>::vector_merge(std::vector<std::string> row_key,
        std::vector<std::string> column_key,
        std::vector<VALUE_T> value) {

    CHECK_EQ(row_key.size(), column_key.size());
    CHECK_EQ(row_key.size(), value.size());
    write_lock _lock(KVStorage::_rwmutex);

    std::string single_key;
    std::vector<std::string> tokens;

    for (uint64_t i = 0; i < row_key.size(); ++i) {
        tokens.clear();
        boost::split(tokens, column_key[i], boost::algorithm::is_any_of("@"));
        single_key.clear();
        single_key.append(row_key[i]);
        single_key.append("!");
        single_key.append(tokens[0]);
        _storage_container[single_key] += value[i];
    }
    return 1;
}

template<class VALUE_T>
int64_t InMemoryKeyValue<VALUE_T>::timed_vector_put(std::vector<std::string> row_key,
        std::vector<std::string> column_key,
        int64_t time_stamp,
        std::vector<VALUE_T> value) {
    CHECK_EQ(row_key.size(), column_key.size());
    CHECK_EQ(row_key.size(), value.size());
    write_lock _lock(KVStorage::_timed_rwmutex);
    for (uint64_t i = 0; i < row_key.size(); ++i) {
        _timed_storage[time_stamp][row_key[i]][column_key[i]] = value[i];
    }
    if (_timed_storage.size() >= 3) {
        _timed_storage.erase(_timed_storage.begin());
    }
    vector_put(row_key, column_key, value);
    return 1;
}

template<class VALUE_T>
void InMemoryKeyValue<VALUE_T>::vector_get(std::vector<std::string> row_key,
        std::vector<std::string> column_key,
        std::vector<VALUE_T>& value) {
    CHECK_EQ(row_key.size(), column_key.size());
    value.clear();
    value.reserve(row_key.size());

    std::string single_key;
    std::string single_value;

    read_lock _lock(KVStorage::_rwmutex);

    for (uint64_t i = 0; i < row_key.size(); ++i) {
        single_key.clear();
        single_value.clear();
        single_key.append(row_key[i]);
        single_key.append("!");
        single_key.append(column_key[i]);

        auto result = _storage_container.find(single_key);
        if ( result == _storage_container.end()) {
            VALUE_T zero_result;
            KVStorage::set_zero(&zero_result);
            value.push_back(zero_result);
        } else {
            value.push_back(result->second);
        }
    }
}

template<class VALUE_T>
void InMemoryKeyValue<VALUE_T>::scan_all(std::map<std::string, std::map<std::string, VALUE_T>>& _return) {
    read_lock _lock(KVStorage::_rwmutex);
    _return.clear();
    std::vector<std::string> tokens;
    std::string single_key;

    for (auto& key : _storage_container) {
        tokens.clear();
        single_key = key.first;
        boost::split(tokens, single_key, boost::algorithm::is_any_of("!"));
        _return[tokens[0]][tokens[1]] = key.second;
    }
}

template<class VALUE_T>
void InMemoryKeyValue<VALUE_T>::timed_scan(int64_t time_stamp,
        std::map<std::string, std::map<std::string, VALUE_T>>& value) {

    read_lock _lock(KVStorage::_timed_rwmutex);
    value.clear();
    value.insert(_timed_storage[time_stamp].begin(), _timed_storage[time_stamp].end());
}

template<class VALUE_T>
std::map<std::string, VALUE_T>* InMemoryKeyValue<VALUE_T>::storage_ptr(){

    return &_storage_container;
}

}
}

#endif

