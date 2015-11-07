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
#include "DAG.h"

namespace ntu {
namespace cap {

int64_t load_file_regular(const std::vector<std::string>& path,
                            const std::vector<int64_t>& size,
                            const std::string& table_name,
                            const std::string& cf_name,
                            const int64_t block_size,
                            std::vector<std::vector<std::pair<std::string, int64_t>>>& full_chuncks) {
    std::vector<std::tuple<std::string, int64_t, int64_t>> free_chuncks;
    CHECK_EQ(path.size(), size.size());
    int64_t chunck_num = 0;
    int64_t last_chunck_size = 0;
    std::vector<std::pair<std::string, int64_t>> new_node;

    for (uint64_t i = 0; i < path.size(); ++i) {
        chunck_num = ceil(size[i]/float(block_size));
        for (int64_t j = 0; j < (chunck_num - 1); ++j) {
            new_node.clear();
            new_node.push_back(std::make_pair(path[i], j));
            full_chuncks.push_back(new_node);
        }
        last_chunck_size = size[i] - block_size * (chunck_num - 1);
        if ((float(last_chunck_size) / block_size) > 0.5) {
            new_node.clear();
            new_node.push_back(std::make_pair(path[i], chunck_num - 1));
            full_chuncks.push_back(new_node);
        } else {
            free_chuncks.push_back(std::make_tuple(path[i], chunck_num - 1, last_chunck_size));
        }
    }

    std::vector<uint64_t> merged_id;
    std::vector<std::pair<std::string, int64_t>> new_chunck;
    while(free_chuncks.size() > 0) {
        merged_id.clear();
        new_chunck.clear();
        int64_t first_ck_size = std::get<2>(free_chuncks[0]);
        int64_t target_ck_size = 0;
        merged_id.push_back(0);
        for (uint64_t i = 1; i < free_chuncks.size(); ++i) {
            target_ck_size = std::get<2>(free_chuncks[i]);
            if ((first_ck_size + target_ck_size) < (1.5 * block_size)) {
                merged_id.push_back(i);
                first_ck_size += target_ck_size;
            } else {
                break;
            }
        }

        for (uint64_t i = 0; i < merged_id.size(); ++i) {
            new_chunck.push_back(std::make_pair(std::get<0>(free_chuncks[i]),
                                                std::get<1>(free_chuncks[i])));
        }
        free_chuncks.erase(free_chuncks.begin(), free_chuncks.begin() + merged_id.size());

        full_chuncks.push_back(new_chunck);
    }
    return 1;
}

int64_t find_hdfs_file(std::vector<std::string>& files,
                    std::vector<int64_t>& sizes,
                    const std::string& path,
                    hdfsFS fs) {
    hdfsFileInfo *check_path;
    check_path = hdfsGetPathInfo(fs, path.c_str());

    if (check_path->mKind == tObjectKind::kObjectKindFile) {
        files.push_back(path);
        sizes.push_back(check_path->mSize);
    } else if (check_path->mKind == tObjectKind::kObjectKindDirectory) {
        hdfsFileInfo *check_dir;
        int numbers;
        check_dir = hdfsListDirectory(fs, path.c_str(), &numbers);
        for (int i = 0; i < numbers; ++i) {
            if (check_dir->mKind == tObjectKind::kObjectKindFile) {
                files.push_back(check_dir->mName);
                sizes.push_back(check_dir->mSize);
            } else if (check_dir->mKind == tObjectKind::kObjectKindDirectory) {
                find_hdfs_file(files, sizes, check_dir->mName, fs);
            }
            ++check_dir;
        }
    } else {
        LOG(FATAL) << "CANNOT DETECT HDFS FILE TYPE";
    }
    return 1;
}

int64_t load_hdfs_file_regular(const std::vector<std::string>& path,
                            const std::vector<int64_t>& size,
                            const std::string& table_name,
                            const std::string& cf_name,
                            const int64_t block_size,
                            StorageType::type storage_type,
                            hdfsFS fs) {
    std::vector<std::vector<std::pair<std::string, int64_t>>> chuncks;

    load_file_regular(path, size, table_name, cf_name, block_size, chuncks);
    /*
    for (auto i : chuncks) {
        for (auto j : i) {
            std::cout << j.first << "->" << j.second << "\n";
        }
        std::cout << "\n";
    }
    */
    KeyPartition hdfs_table_partition;
    hdfs_table_partition.__set_partition_algo(KeyPartitionAlgo::NoneAlgo);
    DAG::create_table(table_name, chuncks.size(), hdfs_table_partition);
    std::string cf_property;
    cf_property.append(cf_name);
    cf_property.append("_hdfs_property");

    DAG::create_cf(table_name, cf_property, StorageType::type::InMemoryKeyValue, ValueType::type::String);

    std::vector<std::string> row;
    std::vector<std::string> column;
    std::vector<std::string> value;

    for (uint64_t shard_id = 0; shard_id < chuncks.size(); ++shard_id) {
        row.clear();
        column.clear();
        value.clear();
        for (auto i : chuncks[shard_id]) {
            row.push_back(i.first);
            column.push_back(std::to_string(i.second));
            value.push_back(std::to_string(block_size));
        }
        Storage::vector_put_string(table_name, cf_property, shard_id, row, column, value);
    }

    DAG::create_cf(table_name, cf_name, storage_type, ValueType::type::String);

    if (storage_type == StorageType::type::HdfsKeyValue) {
        return 1;
    }

    std::shared_ptr<Stage>stage_load_hdfs = std::shared_ptr<Stage>(new Stage());
    stage_load_hdfs->set_function_name("load_hdfs");
    std::vector<std::string> src_cf;
    src_cf.push_back(cf_property);
    stage_load_hdfs->set_src(table_name, src_cf);
    stage_load_hdfs->set_dst(table_name, cf_name);
    int64_t stage_id = Scheduler::singleton().push_back(stage_load_hdfs);

    while(!Scheduler::singleton().check_status(stage_id)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    //std::cout << "load data complete\n";
    return 1;
}


int64_t load_hdfs_file_dir(const std::string& path,
                            const std::string& table_name,
                            const std::string& cf_name,
                            const int64_t block_size,
                            StorageType::type storage_type,
                            hdfsFS fs) {
    std::vector<std::string> files;
    std::vector<int64_t> sizes;
    find_hdfs_file(files, sizes, path, fs);
    load_hdfs_file_regular(files, sizes, table_name, cf_name, block_size, storage_type, fs);
    return 1;
}

int64_t DAG::load_hdfs_file(const std::string& path,
                    const std::string& table_name,
                    const std::string& cf_name,
                    const int64_t block_size,
                    StorageType::type storage_type) {

    struct hdfsBuilder *builder = hdfsNewBuilder();
    hdfsBuilderSetNameNode(builder, NodeInfo::singleton()._hdfs_namenode.c_str());
    hdfsBuilderSetNameNodePort(builder, NodeInfo::singleton()._hdfs_namenode_port);
    hdfsFS fs = hdfsBuilderConnect(builder);

    if (hdfsExists(fs, path.c_str()) == -1) {
        LOG(FATAL) << "CANNOT FIND HDFS PATH " << path;
    }

    hdfsFileInfo *check_path;
    check_path = hdfsGetPathInfo(fs, path.c_str());

    if (check_path->mKind == tObjectKind::kObjectKindFile) {
        std::vector<std::string> regular_path;
        std::vector<int64_t> regular_size;
         regular_path.push_back(path);
         regular_size.push_back(check_path->mSize);
         load_hdfs_file_regular(regular_path,
                             regular_size,
                             table_name,
                             cf_name,
                             block_size,
                             storage_type,
                             fs);
    } else if (check_path->mKind == tObjectKind::kObjectKindDirectory) {
        load_hdfs_file_dir(path, table_name, cf_name, block_size, storage_type, fs);
    } else {
        LOG(FATAL) << "CANNOT DETECT HDFS FILE TYPE";
    }

    hdfsDisconnect(fs);

    return 1;
}

int64_t DAG::save_to_hdfs(const std::string& table_name,
                    const std::string& cf_name) {

    std::shared_ptr<Stage>stage = std::shared_ptr<Stage>(new Stage());
    stage->set_function_name("save_hdfs");
    std::vector<std::string> stage_src_cf;
    stage_src_cf.push_back(cf_name);
    stage->set_src(table_name, stage_src_cf);

    int64_t stage_id = Scheduler::singleton().push_back(stage);

    while(!Scheduler::singleton().check_status(stage_id)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    return 1;
}

}
}
