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

#ifndef NTU_CAP_UNICAP_COMPUTING_USER_CPU_FUNCTIONS_H_
#define NTU_CAP_UNICAP_COMPUTING_USER_CPU_FUNCTIONS_H_

#include "../src/computing/cpu_functions.h"

#include <boost/filesystem.hpp>
#include <boost/algorithm/string/split.hpp>

namespace ntu {
namespace cap {

class UCPUFunctions : public CPUFunctions {
public:
    static UCPUFunctions& singleton() {
        static UCPUFunctions u_cpu_function;
        return u_cpu_function;
    }

    UCPUFunctions() : CPUFunctions() {
        CPUFunctions::_cpu_functions_p["hello_world"] = hello_world;
        CPUFunctions::_cpu_functions_p["word_count_map"] = word_count_map;
        CPUFunctions::_cpu_functions_p["word_count_reduce"] = word_count_reduce;
    }

    static int64_t hello_world (TaskNode new_task) {
        std::cout << "hello world" << "\n";
        std::this_thread::sleep_for(std::chrono::milliseconds(2000));
        return 1;
    }

    static int64_t word_count_map(TaskNode new_task) {

        auto target_storage_ptr = StorageInfo::singleton()._cf_ptr[new_task.src_table_name]
                                                                     [new_task.src_shard_id]
                                                                     [new_task.src_cf_name];

        IntermediateResult<std::string, std::string, int64_t> inter_store("word_count_result");

        auto casted_ptr = std::static_pointer_cast<InMemoryKeyValue<std::string>>(target_storage_ptr);

        for (auto& i: casted_ptr->_storage_container) {
            std::vector<std::string> tokens;
            boost::split(tokens, i.second, boost::algorithm::is_any_of(" / \"\'.<>%&\n"));

            for (auto &j : tokens) {
                if (j.size() != 0)
                    inter_store.push_back(j, "", 1);
            }
        }
        inter_store.merge_data();
        std::vector<std::string> pushed_row_key;
        std::vector<std::string> pushed_col_key;
        std::vector<int64_t>     pushed_value;

        for (uint64_t i = 0; i < inter_store._sharded_result_container.size(); ++i) {
            pushed_row_key.clear();
            pushed_col_key.clear();
            pushed_value.clear();
            for (auto & row_data : inter_store._sharded_result_container[i]) {
                for (auto & col_data : row_data.second) {
                    pushed_row_key.push_back(row_data.first);
                    pushed_col_key.push_back(col_data.first);
                    pushed_value.push_back(col_data.second);
                }
            }

            Storage::vector_put_int(new_task.dst_table_name, new_task.dst_cf_name, i,
                    pushed_row_key, pushed_col_key, pushed_value);
        }
        return 1;
    }

    static int64_t word_count_reduce(TaskNode new_task) {

        auto target_storage_ptr = StorageInfo::singleton()._cf_ptr[new_task.src_table_name]
                                                                     [new_task.src_shard_id]
                                                                     [new_task.src_cf_name];

        auto casted_ptr = std::static_pointer_cast<InMemoryKeyValue<int64_t>>(target_storage_ptr);

        std::vector<std::string> tokens;
        std::unordered_map<std::string, int64_t> result;

        for (auto & i : casted_ptr->_storage_container) {
            tokens.clear();
            boost::split(tokens, i.first, boost::algorithm::is_any_of("!"));
            result[tokens[0]] += i.second;
        }

        std::vector<std::string> pushed_row_key;
        std::vector<std::string> pushed_col_key;
        std::vector<int64_t>     pushed_value;

        for (auto & i : result) {
            pushed_row_key.push_back(i.first);
            pushed_col_key.push_back("");
            pushed_value.push_back(i.second);
        }
        Storage::vector_put_int(new_task.dst_table_name, new_task.dst_cf_name, new_task.src_shard_id,
                           pushed_row_key, pushed_col_key, pushed_value);

        return 1;
    }


};

}
}


#endif /* NTU_CAP_UNICAP_COMPUTING_USER_CPU_FUNCTIONS_H_ */
