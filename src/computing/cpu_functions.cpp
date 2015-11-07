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

#include "cpu_functions.h"

namespace ntu {
namespace cap {

CPUFunctions::CPUFunctions() {
    _cpu_functions_p["test"] = test;
    _cpu_functions_p["load_hdfs"] = load_hdfs;
    _cpu_functions_p["save_hdfs"] = save_hdfs;
    _cpu_functions_p["load_distributed_cache"] = load_distributed_cache;
    _cpu_functions_p["load_hdfs_image"] = load_hdfs_image;
}

int64_t CPUFunctions::test (TaskNode new_task) {
    std::cout << "test the function\n";
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));
    return 1;
}

int64_t CPUFunctions::load_hdfs (TaskNode new_task) {

    struct hdfsBuilder *builder = hdfsNewBuilder();
    hdfsBuilderSetForceNewInstance(builder);
    hdfsBuilderSetNameNode(builder, NodeInfo::singleton()._hdfs_namenode.c_str());
    hdfsBuilderSetNameNodePort(builder, NodeInfo::singleton()._hdfs_namenode_port);
    hdfsFS fs = hdfsBuilderConnect(builder);

    std::map<std::string, std::map<std::string, std::string>> hdfs_property;

    auto target_storage_ptr = StorageInfo::singleton()._cf_ptr[new_task.src_table_name]
                                                             [new_task.src_shard_id]
                                                             [new_task.src_cf_name];
    auto casted_ptr = std::static_pointer_cast<InMemoryKeyValue<std::string>>(target_storage_ptr);
    casted_ptr->scan_all(hdfs_property);

    int64_t buffer_size = 1024;
    char buffer[buffer_size];
    std::vector<std::string> row;
    std::vector<std::string> column;
    std::vector<std::string> value;
    std::string single_value;

    for (auto& files_hdfs : hdfs_property) {
        memset(buffer, 0, buffer_size);
        std::string path = files_hdfs.first;
        for (auto& shards_hdfs : files_hdfs.second) {
            int shard_id = std::stoi (shards_hdfs.first, nullptr, 10);
            int block_size = std::stoi (shards_hdfs.second,nullptr, 10);

            row.push_back(path);
            column.push_back(shards_hdfs.first);
            single_value.clear();

            hdfsFile file = hdfsOpenFile(fs, path.c_str(), O_RDONLY, 0, 0, 0);
            hdfsSeek(fs, file, block_size * shard_id);

            if (shard_id != 0) {
                while (hdfsRead(fs, file, buffer, 1)) {
                    if (buffer[0] == '\n') {
                        break;
                    }
                }
            }

            int64_t total_read_size = 0;
            int64_t each_read_size = 0;
            bool    eof = false;
            while(!eof) {
                memset(buffer, 0, buffer_size);
                if ((total_read_size + buffer_size) >= block_size) {
                    each_read_size = hdfsRead(fs, file, buffer, buffer_size);
                    single_value.append(std::string(buffer, each_read_size));
                    total_read_size += each_read_size;
                    break;
                }
                each_read_size = hdfsRead(fs, file, buffer, buffer_size);
                if (each_read_size == 0) {
                    eof = true;
                    break;
                }
                single_value.append(std::string(buffer, each_read_size));
                total_read_size += each_read_size;
            }
            while (!eof) {
                memset(buffer, 0, buffer_size);
                each_read_size = hdfsRead(fs, file, buffer, buffer_size);
                if (each_read_size == 0) {
                     eof = true;
                     break;
                 }
                std::string check_buffer(buffer, each_read_size);
                std::size_t found = check_buffer.find("\n");

                if (found == std::string::npos) {
                    single_value.append(std::string(buffer, each_read_size));
                } else {
                    single_value.append(std::string(buffer, found));
                    break;
                }
            }
            value.push_back(single_value);
            hdfsCloseFile(fs, file);
        }
    }
    auto dst_storage_ptr = StorageInfo::singleton()._cf_ptr[new_task.dst_table_name]
                                     [new_task.src_shard_id]
                                     [new_task.dst_cf_name];
    auto dst_casted_ptr = std::static_pointer_cast<InMemoryKeyValue<std::string>>(dst_storage_ptr);
    dst_casted_ptr->vector_put(row, column, value);
    hdfsDisconnect(fs);
    return 1;
}

int64_t CPUFunctions::load_distributed_cache (TaskNode new_task) {
    std::string cached_table = new_task.dst_table_name;
    std::string cached_cf    = new_task.dst_cf_name;

    std::string table        = new_task.src_table_name;
    std::string cf           = new_task.src_cf_name;
    int64_t shard_id         = new_task.src_shard_id;

    int64_t cached_shard_num = Storage::get_shard_num(cached_table);

    auto value_type = StorageInfo::singleton()._cf_info[cached_table][cached_cf]._cf_property.value_type;
    std::map<std::string,  std::map<std::string, int64_t>> value_int;
    std::map<std::string,  std::map<std::string, double>> value_double;
    std::map<std::string,  std::map<std::string, std::string>> value_string;

    for (int64_t i = 0; i < cached_shard_num; ++i) {
        switch (value_type) {
        case ValueType::type::Int64: {
            Storage::scan_int(cached_table, cached_cf, i, value_int);

            if (value_int.size() > 0) {
                std::vector<std::string> row_key;
                std::vector<std::string> col_key;
                std::vector<int64_t> values;

                for (auto& row_record : value_int) {
                    for (auto& col_record : row_record.second) {
                        row_key.push_back(row_record.first);
                        col_key.push_back(col_record.first);
                        values.push_back(col_record.second);
                    }
                }
                Storage::vector_put_int(table, cf, shard_id, row_key, col_key, values);
            }
            break;
        }
        case ValueType::type::Double: {
            Storage::scan_double(cached_table, cached_cf, i, value_double);

            if (value_int.size() > 0) {
                std::vector<std::string> row_key;
                std::vector<std::string> col_key;
                std::vector<double> values;

                for (auto& row_record : value_double) {
                    for (auto& col_record : row_record.second) {
                        row_key.push_back(row_record.first);
                        col_key.push_back(col_record.first);
                        values.push_back(col_record.second);
                    }
                }
                Storage::vector_put_double(table, cf, shard_id, row_key, col_key, values);
            }
            break;
        }
        case ValueType::type::String: {
            Storage::scan_string(cached_table, cached_cf, i, value_string);

            if (value_int.size() > 0) {
                std::vector<std::string> row_key;
                std::vector<std::string> col_key;
                std::vector<std::string> values;

                for (auto& row_record : value_string) {
                    for (auto& col_record : row_record.second) {
                        row_key.push_back(row_record.first);
                        col_key.push_back(col_record.first);
                        values.push_back(col_record.second);
                    }
                }
                Storage::vector_put_string(table, cf, shard_id, row_key, col_key, values);
            }
            break;
        }
        default:
            break;
        }
    }
    return 1;
}

int64_t CPUFunctions::save_hdfs (TaskNode new_task) {
    struct hdfsBuilder *builder = hdfsNewBuilder();
    hdfsBuilderSetForceNewInstance(builder);
    hdfsBuilderSetNameNode(builder, NodeInfo::singleton()._hdfs_namenode.c_str());
    hdfsBuilderSetNameNodePort(builder, NodeInfo::singleton()._hdfs_namenode_port);
    hdfsFS fs = hdfsBuilderConnect(builder);

    std::string target_dir;
    target_dir.append(NodeInfo::singleton()._root_dir);
    if (target_dir.back() != '/') {
        target_dir.append("/");
    }
    target_dir.append(NodeInfo::singleton()._app_name);
    target_dir.append("/");
    target_dir.append(new_task.src_table_name);
    target_dir.append("/");
    target_dir.append(new_task.src_cf_name);

    if (hdfsExists(fs, target_dir.c_str()) == -1) {
        hdfsCreateDirectory(fs, target_dir.c_str());
    }

    std::string target_path;
    target_path.append(target_dir);
    target_path.append("/");
    target_path.append(std::to_string(new_task.src_shard_id));

    if (hdfsExists(fs, target_dir.c_str()) == 0) {
        hdfsDelete(fs, target_path.c_str(), 0);
    }

    hdfsFile file = hdfsOpenFile(fs, target_path.c_str(), O_WRONLY, 0, 0, 0);

    std::map<std::string,  std::map<std::string, int64_t>> value_int;
    std::map<std::string,  std::map<std::string, double>> value_double;
    std::map<std::string,  std::map<std::string, std::string>> value_string;

    auto value_type = StorageInfo::singleton().
            _cf_info[new_task.src_table_name]
            [new_task.src_cf_name].
            _cf_property.
            value_type;

    switch (value_type) {

    case ValueType::type::Int64: {
        Storage::scan_int(new_task.src_table_name,
                new_task.src_cf_name,
                new_task.src_shard_id,
                value_int);
        for (auto& i : value_int) {
            for (auto& j : i.second) {
                hdfsWrite(fs, file, i.first.c_str(), i.first.size());
                hdfsWrite(fs, file, "!", 1);
                hdfsWrite(fs, file, j.first.c_str(), j.first.size());
                hdfsWrite(fs, file, "!", 1);
                std::string single_value = std::to_string(j.second);
                hdfsWrite(fs, file, single_value.c_str(), single_value.size());
                hdfsWrite(fs, file, "\n", 1);
            }
        }
        break;
    }
    case ValueType::type::Double: {
        Storage::scan_double(new_task.src_table_name,
                new_task.src_cf_name,
                new_task.src_shard_id,
                value_double);
        for (auto& i : value_double) {
            for (auto& j : i.second) {
                hdfsWrite(fs, file, i.first.c_str(), i.first.size());
                hdfsWrite(fs, file, "!", 1);
                hdfsWrite(fs, file, j.first.c_str(), j.first.size());
                hdfsWrite(fs, file, "!", 1);
                std::string single_value = std::to_string(j.second);
                hdfsWrite(fs, file, single_value.c_str(), single_value.size());
                hdfsWrite(fs, file, "\n", 1);
            }
        }
        break;
    }
    case ValueType::type::String: {
        Storage::scan_string(new_task.src_table_name,
                new_task.src_cf_name,
                new_task.src_shard_id,
                value_string);

        for (auto& i : value_string) {
            for (auto& j : i.second) {
                hdfsWrite(fs, file, i.first.c_str(), i.first.size());
                hdfsWrite(fs, file, "!", 1);
                hdfsWrite(fs, file, j.first.c_str(), j.first.size());
                hdfsWrite(fs, file, "!", 1);
                hdfsWrite(fs, file, j.second.c_str(), j.second.size());
                hdfsWrite(fs, file, "\n", 1);
            }
        }
        break;
    }
    default:
        break;
    }

    hdfsCloseFile(fs, file);
    hdfsDisconnect(fs);

    return 1;
}

int64_t CPUFunctions::load_hdfs_image(TaskNode new_task) {

    struct hdfsBuilder *builder = hdfsNewBuilder();
    hdfsBuilderSetForceNewInstance(builder);
    hdfsBuilderSetNameNode(builder, NodeInfo::singleton()._hdfs_namenode.c_str());
    hdfsBuilderSetNameNodePort(builder, NodeInfo::singleton()._hdfs_namenode_port);
    hdfsFS fs = hdfsBuilderConnect(builder);

    std::map<std::string, std::map<std::string, std::string>> hdfs_property;

    auto src_ptr = StorageInfo::singleton()._cf_ptr[new_task.src_table_name]
                                 [new_task.src_shard_id]
                                 [new_task.src_cf_name];
    auto src_casted_ptr = std::static_pointer_cast<InMemoryKeyValue<std::string>>(src_ptr);
    src_casted_ptr->scan_all(hdfs_property);

    int64_t buffer_size = 1024;
    char buffer[buffer_size];
    std::vector<std::string> row;
    std::vector<std::string> column;
    std::vector<std::string> value;
    std::string single_value;
    int64_t total_size = 0;

    for (auto& files_hdfs : hdfs_property) {
        memset(buffer, 0, buffer_size);
        std::string path = files_hdfs.first;
        for (auto& shards_hdfs : files_hdfs.second) {
            row.push_back(path);
            column.push_back(shards_hdfs.first);

            hdfsFile file = hdfsOpenFile(fs, path.c_str(), O_RDONLY, 0, 0, 0);

            bool    eof = false;
            int64_t read_size = 0;
            while(!eof) {
                memset(buffer, 0, buffer_size);
                read_size = hdfsRead(fs, file, buffer, buffer_size);
                if (read_size == 0) {
                    eof = true;
                    break;
                }
                single_value.append(buffer, read_size);
            }
            value.push_back(single_value);
            hdfsCloseFile(fs, file);
            total_size += single_value.size();
        }
    }
    auto storage_ptr = StorageInfo::singleton()._cf_ptr[new_task.dst_table_name]
                                         [new_task.src_shard_id]
                                         [new_task.dst_cf_name];
    auto casted_ptr = std::static_pointer_cast<InMemoryImage<std::string>>(storage_ptr);
    casted_ptr->vector_put(row, column, value);

    hdfsDisconnect(fs);
    return 1;
}

}
}


