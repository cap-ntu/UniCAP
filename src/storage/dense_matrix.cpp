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

#ifdef DENSE_MATRIX

#include <unordered_map>
#include <map>
#include "./kv_base.h"
#include "../tools/include/Eigen/Dense"

namespace ntu {
namespace cap {

template <class VALUE_T>
DenseMatrix<VALUE_T>::DenseMatrix():KVStorage() {
}

template <class VALUE_T>
DenseMatrix<VALUE_T>::DenseMatrix(const std::string table_name,
        const int64_t shard_id,
        const std::string cf_name,
        std::pair<int64_t, int64_t> size):
        KVStorage(table_name, shard_id, cf_name) {

    _storage_container.resize(size.first, size.second);
    _storage_container.setRandom();
    //_storage_container.setZero();
}

template <class VALUE_T>
DenseMatrix<VALUE_T>::~DenseMatrix() {
}

template <class VALUE_T>
int64_t DenseMatrix<VALUE_T>::vector_put(std::vector<std::string> row_key,
        std::vector<std::string> column_key,
        std::vector<VALUE_T> value) {

    CHECK_EQ(row_key.size(), column_key.size());
    CHECK_EQ(row_key.size(), value.size());
    write_lock _lock(KVStorage::_rwmutex);
    int64_t row = 0;
    int64_t column = 0;

    for (uint64_t i = 0; i < row_key.size(); ++i) {
        row = std::stol(row_key[i]);
        column = std::stol(column_key[i]);
        _storage_container(row, column) = value[i];
    }
    return 1;
}

template <class VALUE_T>
int64_t DenseMatrix<VALUE_T>::vector_merge(std::vector<std::string> row_key,
        std::vector<std::string> column_key,
        std::vector<VALUE_T> value) {

    CHECK_EQ(row_key.size(), column_key.size());
    CHECK_EQ(row_key.size(), value.size());
    write_lock _lock(KVStorage::_rwmutex);
    int64_t row = 0;
    int64_t column = 0;

    for (uint64_t i = 0; i < row_key.size(); ++i) {
        row = std::stol(row_key[i]);
        column = std::stol(column_key[i]);
        _storage_container(row, column) += value[i];
    }
    return 1;
}

template <class VALUE_T>
int64_t DenseMatrix<VALUE_T>::timed_vector_put(std::vector<std::string> row_key,
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

template <class VALUE_T>
void DenseMatrix<VALUE_T>::vector_get(std::vector<std::string> row_key,
        std::vector<std::string> column_key,
        std::vector<VALUE_T>& value) {

    CHECK_EQ(row_key.size(), column_key.size());
    value.clear();

    int64_t row = 0;
    int64_t column = 0;

    read_lock _lock(KVStorage::_rwmutex);
    value.reserve(row_key.size());

    for (uint64_t i = 0; i < row_key.size(); ++i) {
        row = std::stol(row_key[i]);
        column = std::stol(column_key[i]);

        if (row >= _storage_container.rows() ||
                column >= _storage_container.cols()) {
            LOG(ERROR) << "DENSE MATRIX INDEX ERROR";
        }
        value.push_back(_storage_container(row, column));
    }
}

template <class VALUE_T>
void DenseMatrix<VALUE_T>::scan_all(std::map<std::string, std::map<std::string, VALUE_T>>& value) {
    read_lock _lock(KVStorage::_rwmutex);
    value.clear();

    int64_t row = _storage_container.rows();
    int64_t column = _storage_container.cols();

    for (int64_t i = 0; i < row; ++i) {
        for (int64_t j = 0; j < column; ++j) {
            value[std::to_string(i)][std::to_string(j)] = _storage_container(i, j);
        }
    }
}

template <class VALUE_T>
void DenseMatrix<VALUE_T>::timed_scan(int64_t time_stamp,
        std::map<std::string, std::map<std::string, VALUE_T>>& value) {

    read_lock _lock(KVStorage::_timed_rwmutex);
    value.clear();
    value.insert(_timed_storage[time_stamp].begin(), _timed_storage[time_stamp].end());
}

template <class VALUE_T>
Eigen::Matrix<VALUE_T, Eigen::Dynamic, Eigen::Dynamic>* DenseMatrix<VALUE_T>::storage_ptr(){

    return &_storage_container;
}


}
}

#endif
