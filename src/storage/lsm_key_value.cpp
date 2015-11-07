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

#ifdef LSM_KEY_VALUE

#include "./kv_base.h"
#include "../tools/include/leveldb/db.h"

namespace ntu {
namespace cap {

template<class VALUE_T>
LSMKeyValue<VALUE_T>::LSMKeyValue():KVStorage() {
}

template<class VALUE_T>
LSMKeyValue<VALUE_T>::LSMKeyValue(const std::string table_name,
        const int64_t shard_id,
        const std::string cf_name):
        KVStorage(table_name, shard_id, cf_name) {

    _path = "./lsm_key_value/";
    _path.append(table_name);
    _path.append("/");
    _path.append(std::to_string(shard_id));
    _path.append(cf_name);
    boost::filesystem::create_directories(_path);
    _options.create_if_missing = true;

    //  boost::filesystem::remove_all(_path);

    leveldb::Status status = leveldb::DB::Open(_options, _path, &_db);
    CHECK_EQ(status.ok(), true);
}

template<class VALUE_T>
LSMKeyValue<VALUE_T>::~LSMKeyValue() {
}

template<class VALUE_T>
int64_t LSMKeyValue<VALUE_T>::vector_put(std::vector<std::string> row_key,
        std::vector<std::string> column_key,
        std::vector<VALUE_T> value) {

    CHECK_EQ(row_key.size(), column_key.size());
    CHECK_EQ(row_key.size(), value.size());

    std::string single_key;
    for (uint64_t i = 0; i < row_key.size(); ++i) {
        single_key.clear();
        single_key.append(row_key[i]);
        single_key.append("!");
        single_key.append(column_key[i]);
        _db->Put(leveldb::WriteOptions(), single_key, KVStorage::to_string(value[i]));
    }
    return 1;
}

template<class VALUE_T>
int64_t LSMKeyValue<VALUE_T>::vector_merge(std::vector<std::string> row_key,
        std::vector<std::string> column_key,
        std::vector<VALUE_T> value) {

    CHECK_EQ(row_key.size(), column_key.size());
    CHECK_EQ(row_key.size(), value.size());

    std::string single_key;
    std::string old_value;
    VALUE_T old_value_t;

    std::vector<std::string> tokens;
    leveldb::Status s;

    for (uint64_t i = 0; i < row_key.size(); ++i) {
        single_key.clear();
        tokens.clear();
        old_value.clear();
        boost::split(tokens, column_key[i], boost::algorithm::is_any_of("@"));
        single_key.append(row_key[i]);
        single_key.append("!");
        single_key.append(tokens[0]);

        s = _db->Get(leveldb::ReadOptions(), single_key, &old_value);
        if (s.ok()) {
           KVStorage::to_value_t(old_value, &old_value_t);
           _db->Put(leveldb::WriteOptions(), single_key, KVStorage::to_string(old_value_t + value[i]));
        } else {
            _db->Put(leveldb::WriteOptions(), single_key, KVStorage::to_string(value[i]));
        }
    }
    return 1;
}

template<class VALUE_T>
int64_t LSMKeyValue<VALUE_T>::timed_vector_put(std::vector<std::string> row_key,
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
void LSMKeyValue<VALUE_T>::vector_get(std::vector<std::string> row_key,
                            std::vector<std::string> column_key,
                            std::vector<VALUE_T>& value) {
    CHECK_EQ(row_key.size(), column_key.size());
    value.clear();
    value.reserve(row_key.size());

    std::string single_key;
    std::string single_value;
    VALUE_T value_t;
    leveldb::Status s;

    for (uint64_t i = 0; i < row_key.size(); ++i) {
        single_key.clear();
        single_value.clear();

        single_key.append(row_key[i]);
        single_key.append("!");
        single_key.append(column_key[i]);

        s = _db->Get(leveldb::ReadOptions(), single_key, &single_value);
        if (s.ok()) {
            KVStorage::to_value_t(single_value, &value_t);
            value.push_back(value_t);
        } else {
            KVStorage::set_zero(&value_t);
            value.push_back(value_t);
        }
    }
}

template<class VALUE_T>
void LSMKeyValue<VALUE_T>::scan_all(std::map<std::string, std::map<std::string, VALUE_T>>& value) {
    value.clear();

    std::vector<std::string> tokens;
    std::string single_key;
    std::string single_value;
    VALUE_T value_t;

    leveldb::Iterator* it = _db->NewIterator(leveldb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        tokens.clear();
        single_key = it->key().ToString();
        single_value = it->value().ToString();

        boost::split(tokens, single_key, boost::algorithm::is_any_of("!"));

        KVStorage::to_value_t(single_value, &value_t);
        value[tokens[0]][tokens[1]] = value_t;
    }
    delete it;
}

template<class VALUE_T>
void LSMKeyValue<VALUE_T>::timed_scan(int64_t time_stamp,
        std::map<std::string, std::map<std::string, VALUE_T>>& value) {
    read_lock _lock(KVStorage::_timed_rwmutex);
    value.insert(_timed_storage[time_stamp].begin(), _timed_storage[time_stamp].end());
}

template<class VALUE_T>
leveldb::DB* LSMKeyValue<VALUE_T>::storage_ptr(){
    return _db;
}

}
}

#endif


