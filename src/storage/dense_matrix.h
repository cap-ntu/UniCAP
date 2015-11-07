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

#ifndef UNICAP_SRC_STORAGE_DENSE_MATRIX_H_
#define UNICAP_SRC_STORAGE_DENSE_MATRIX_H_

#include <unordered_map>
#include <map>
#include "./kv_base.h"
#include "../tools/include/Eigen/Dense"

namespace ntu {
namespace cap {

template <class VALUE_T>
class DenseMatrix : public KVStorage {

public:

    DenseMatrix();

    DenseMatrix(const std::string table_name,
            const int64_t shard_id,
            const std::string cf_name,
            std::pair<int64_t, int64_t> size);

    ~DenseMatrix();

    int64_t vector_put(std::vector<std::string> row_key,
            std::vector<std::string> column_key,
            std::vector<VALUE_T> value);

    int64_t vector_merge(std::vector<std::string> row_key,
            std::vector<std::string> column_key,
            std::vector<VALUE_T> value);


    int64_t timed_vector_put(std::vector<std::string> row_key,
            std::vector<std::string> column_key,
            int64_t time_stamp,
            std::vector<VALUE_T> value);

    void vector_get(std::vector<std::string> row_key,
            std::vector<std::string> column_key,
            std::vector<VALUE_T>& value);

    void scan_all(std::map<std::string, std::map<std::string, VALUE_T>>& value);

    void timed_scan(int64_t time_stamp, std::map<std::string, std::map<std::string, VALUE_T>>& value);

    Eigen::Matrix<VALUE_T, Eigen::Dynamic, Eigen::Dynamic>* storage_ptr();

    Eigen::Matrix<VALUE_T, Eigen::Dynamic, Eigen::Dynamic> _storage_container;

    typedef std::map<std::string, std::map<std::string, VALUE_T>> TIMED_DATA;
    std::map<int64_t, TIMED_DATA> _timed_storage;
};


}
}

#define DENSE_MATRIX
#include "./dense_matrix.cpp"

#endif /* UNICAP_SRC_STORAGE_DENSE_MATRIX_H_ */
