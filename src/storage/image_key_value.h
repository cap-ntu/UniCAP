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

#ifndef NTU_CAP_UNICAP_STORAGE_IMAGE_KEY_VALUE_
#define NTU_CAP_UNICAP_STORAGE_IMAGE_KEY_VALUE_

#include <unordered_map>
#include <map>
#include "../tools/include/opencv2/opencv.hpp"
#include "./kv_base.h"

namespace ntu {
namespace cap {

template<class VALUE_T>
class InMemoryImage : public KVStorage {

public:

    InMemoryImage();

    InMemoryImage(const std::string table_name,
                    const int64_t shard_id,
                    const std::string cf_name);

    ~InMemoryImage();

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

    std::map<std::string, cv::Mat>* storage_ptr();

    std::map<std::string, cv::Mat> _storage_container;

    typedef std::map<std::string, std::map<std::string, VALUE_T>> TIMED_DATA;
    std::map<int64_t, TIMED_DATA> _timed_storage;
};

}
}

#define IMAGE_KEY_VALUE
#include "image_key_value.cpp"

#endif /* NTU_CAP_UNICAP_STORAGE_IMAGE_KEY_VALUE_ */
