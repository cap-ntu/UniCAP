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

#ifdef IMAGE_KEY_VALUE

#include <unordered_map>
#include <map>
#include "../tools/include/opencv2/opencv.hpp"
#include "./kv_base.h"

namespace ntu {
namespace cap {

typedef unsigned char byte;

template<class VALUE_T>
InMemoryImage<VALUE_T>::InMemoryImage():KVStorage() {
}

template<class VALUE_T>
InMemoryImage<VALUE_T>::InMemoryImage(const std::string table_name,
                                const int64_t shard_id,
                                const std::string cf_name):
                                KVStorage(table_name, shard_id, cf_name) {
}

template<class VALUE_T>
InMemoryImage<VALUE_T>::~InMemoryImage() {
}

template<class VALUE_T>
int64_t InMemoryImage<VALUE_T>::vector_put(std::vector<std::string> row_key,
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

        std::vector<byte> vectordata(value[i].begin(), value[i].end());
        cv::Mat data_mat(vectordata, false);
        auto image = cv::Mat(cv::imdecode(data_mat, CV_LOAD_IMAGE_COLOR));
        if (!image.empty()) {
            _storage_container[single_key] = image;
        }
    }
    return 1;
}

template<class VALUE_T>
int64_t InMemoryImage<VALUE_T>::vector_merge(std::vector<std::string> row_key,
                   std::vector<std::string> column_key,
                   std::vector<VALUE_T> value) {
    CHECK_EQ(row_key.size(), column_key.size());
    CHECK_EQ(row_key.size(), value.size());
    write_lock _lock(KVStorage::_rwmutex);
    LOG(FATAL) << "NOT IMPLEMENTED \n";
    return 1;
}

template<class VALUE_T>
int64_t InMemoryImage<VALUE_T>::timed_vector_put(std::vector<std::string> row_key,
                          std::vector<std::string> column_key,
                          int64_t time_stamp,
                          std::vector<VALUE_T> value) {
    CHECK_EQ(row_key.size(), column_key.size());
    CHECK_EQ(row_key.size(), value.size());
    write_lock _lock(KVStorage::_rwmutex);
    LOG(FATAL) << "NOT IMPLEMENTED \n";
    return 1;
}

template<class VALUE_T>
void InMemoryImage<VALUE_T>::vector_get(std::vector<std::string> row_key,
                                std::vector<std::string> column_key,
                                std::vector<VALUE_T>& value) {
    CHECK_EQ(row_key.size(), column_key.size());
    value.clear();
    std::string single_key;
    std::string single_value;
    read_lock _lock(KVStorage::_rwmutex);
    LOG(FATAL) << "NOT IMPLEMENTED \n";

}

template<class VALUE_T>
void InMemoryImage<VALUE_T>::scan_all(std::map<std::string, std::map<std::string, VALUE_T>>& value) {
    read_lock _lock(KVStorage::_rwmutex);
    value.clear();

    std::vector<std::string> tokens;
    std::string single_key;
    std::string single_value;

    LOG(FATAL) << "NOT IMPLEMENTED \n";
}

template<class VALUE_T>
void InMemoryImage<VALUE_T>::timed_scan(int64_t time_stamp,
        std::map<std::string, std::map<std::string, VALUE_T>>& value) {
    read_lock _lock(KVStorage::_rwmutex);
    value.clear();
    LOG(FATAL) << "NOT IMPLEMENTED \n";

}

template<class VALUE_T>
std::map<std::string, cv::Mat>* InMemoryImage<VALUE_T>::storage_ptr(){

    return &_storage_container;
}

}
}

#endif

