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
#ifndef NTU_CAP_UNICAP_STORAGE_KV_BASE_H_
#define NTU_CAP_UNICAP_STORAGE_KV_BASE_H_

#include <map>
#include <unordered_map>
#include <vector>
#include <list>
#include <string>
#include <mutex>
#include <atomic>
#include <boost/thread/shared_mutex.hpp>
#include <boost/thread/shared_lock_guard.hpp>
#include <boost/thread.hpp>
#include <boost/filesystem.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <glog/logging.h>

#include "../common/node_info.h"

typedef boost::shared_mutex rwmutex;
typedef boost::shared_lock<rwmutex> read_lock;
typedef boost::unique_lock<rwmutex> write_lock;

namespace ntu {
namespace cap {

class KVStorage {
public:
    KVStorage() {
    }

    KVStorage(const std::string table_name,
            const int64_t shard_id,
            const std::string cf_name) {
        _table_name = table_name;
        _cf_name = cf_name;
        _shard_id = shard_id;
    }

    virtual ~KVStorage() {
    }

    void set_zero(int64_t *value) {
        *value = 0;
    }

    void set_zero(double *value) {
        *value = 0;
    }

    void set_zero(std::string *value) {
        value->clear();
    }

    std::string to_string(int64_t src) {
        return (std::to_string(src));
    }

    std::string to_string(double src) {
        return (std::to_string(src));
    }

    std::string to_string(std::string& src) {
        return src;
    }

    void to_value_t(std::string& src, int64_t *dst) {
        *dst = std::stol(src);
    }

    void to_value_t(std::string& src, double *dst) {
        *dst = std::stod(src);
    }

    void to_value_t(std::string& src, std::string *dst) {
        *dst = src;
    }

    // std::mutex _lock;
    rwmutex _rwmutex;
    rwmutex _timed_rwmutex;
    std::string _table_name;
    int64_t _shard_id;
    std::string _cf_name;
};

}
}
#endif /* UNICAP_STORAGE_KV_BASE_H_ */
