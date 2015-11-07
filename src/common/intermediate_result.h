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

#ifndef UNICAP_COMMON_INTERMEDIATE_RESULT_H_
#define UNICAP_COMMON_INTERMEDIATE_RESULT_H_

#include "storage.h"

namespace ntu {
namespace cap {

template <class T_KEY, class T_COLUMN, class T_VALUE>
class IntermediateResult {
public:

    IntermediateResult(std::string table_name) {

        if (StorageInfo::singleton()._table_info.find(table_name)
                == StorageInfo::singleton()._table_info.end()) {
            LOG(FATAL) << "CANNOT FIND TABLE " << table_name;
        }

        _shard_num = Storage::get_shard_num(table_name);
        _partition = Storage::get_table_partition(table_name);
        _sharded_result_container.resize(_shard_num);
    }

    int64_t push_back(T_KEY row_key,
                    T_COLUMN column_key,
                    T_VALUE value) {
        _intermediate_result_container[row_key][column_key].push_back(value);
        return 1;
    }

    std::string to_string(int value) {
        return std::to_string(value);
    }

    std::string to_string(long value) {
        return std::to_string(value);
    }

    std::string to_string(long long value) {
        return std::to_string(value);
    }

    std::string to_string(unsigned value) {
        return std::to_string(value);
    }

    std::string to_string(unsigned long value) {
        return std::to_string(value);
    }

    std::string to_string(unsigned long long value) {
        return std::to_string(value);
    }

    std::string to_string(float value) {
        return std::to_string(value);
    }

    std::string to_string(double value) {
        return std::to_string(value);
    }

    std::string to_string(long double value) {
        return std::to_string(value);
    }

    std::string to_string(std::string value) {
        return value;
    }

    void set_zero(int* value) {
        *value = 0;
    }

    void set_zero(long* value) {
        *value = 0;
    }

    void set_zero(long long* value) {
        *value = 0;
    }

    void set_zero(unsigned* value) {
        *value = 0;
    }

    void set_zero(unsigned long* value) {
        *value = 0;
    }

    void set_zero(unsigned long long* value) {
        *value = 0;
    }

    void set_zero(float* value) {
        *value = 0;
    }

    void set_zero(double* value) {
        *value = 0;
    }

    void set_zero(long double* value) {
        *value = 0;
    }

    void set_zero(std::string* value) {
        value->clear();
    }

    int64_t merge_data() {
        T_VALUE merged_value;
        std::string row_string;
        std::string column_string;
        std::string value;
        int64_t target_shard = 0;

        for (auto& row_data : _intermediate_result_container) {
            for (auto& column_data : row_data.second) {

                set_zero(&merged_value);
                row_string = to_string(row_data.first);
                column_string = to_string(column_data.first);
                column_string.append("@");
                column_string.append(std::to_string(NodeInfo::singleton()._node_id));

                for (auto& inter_result : column_data.second) {
                    merged_value += inter_result;
                }

                if (_partition.partition_algo ==
                        KeyPartitionAlgo::HashingPartition) {

                    target_shard = _hash_fn(row_string) % _shard_num;
                    _sharded_result_container[target_shard][row_string][column_string] = merged_value;

                } else if (_partition.partition_algo ==
                        KeyPartitionAlgo::NoneAlgo) {

                    LOG(ERROR) << "NON KEY PARTITION ALGO IN THIS TABLE";

                } else if (_partition.partition_algo ==
                        KeyPartitionAlgo::RangePartition) {

                    auto it = _partition.key_to_shard.lower_bound(row_string);
                    target_shard = it->second;
                    _sharded_result_container[target_shard][row_string][column_string] = merged_value;
                }

                //_merged_result_container[row_string][column_string] = value_string;
                column_data.second.clear();
            }
        }
        return 1;
    }
    typedef std::vector<T_VALUE> ValueData;
    typedef std::unordered_map<T_COLUMN, ValueData> ColumnData;
    typedef std::unordered_map<T_KEY, ColumnData> RowData;
    typedef std::unordered_map<std::string, T_VALUE> MergedColumnData;
    typedef std::unordered_map<std::string, MergedColumnData> MergedRowData;
    typedef std::vector<MergedRowData> ShardedRowData;

    RowData _intermediate_result_container;
    MergedRowData _merged_result_container;
    ShardedRowData _sharded_result_container;

    int64_t _shard_num;
    KeyPartition _partition;

    std::hash<std::string> _hash_fn;
};

}
}


#endif /* UNICAP_COMMON_INTERMEDIATE_RESULT_H_ */
