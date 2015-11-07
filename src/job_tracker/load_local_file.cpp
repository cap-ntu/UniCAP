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

int64_t load_local_file_regular(const std::vector<std::string>& path,
                    const std::vector<int64_t>& size,
                    const std::string& table_name,
                    const std::string& cf_name,
                    const int64_t block_size,
                    StorageType::type storage_type) {

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
    int64_t total_file_size = 0;

    for (auto i : path) {
        boost::filesystem::path check_file(i);
        if (!boost::filesystem::is_regular(check_file)) {
            LOG(FATAL) << i << " IS NOT REGULAR DATA";
        }
        total_file_size += boost::filesystem::file_size(check_file);
    }
    KeyPartition local_file_partition;
    local_file_partition.__set_partition_algo(KeyPartitionAlgo::NoneAlgo);
    DAG::create_table(table_name, chuncks.size(), local_file_partition);
    DAG::create_cf(table_name, cf_name, storage_type, ValueType::type::String);
    std::vector<std::string> row;
    std::vector<std::string> column;
    int64_t buffer_size = 1024;
    char buffer[buffer_size];
    std::string single_value;
    std::vector<std::string> value;


    for (uint64_t shard_id = 0; shard_id < chuncks.size(); ++shard_id) {
        row.clear();
        column.clear();
        value.clear();
        for (auto i : chuncks[shard_id]) {
            memset(buffer, 0, buffer_size);
            single_value.clear();
            std::ifstream data(i.first);

            row.push_back(i.first);
            column.push_back(std::to_string(i.second));
            data.seekg(block_size * i.second);

            int64_t read_size = 0;

            if (i.second != 0) {
                std::string old_line;
                std::getline(data, old_line, '\n');
            }

            while (!data.eof()) {
                memset(buffer, 0, buffer_size);
                if ((read_size + buffer_size) >= block_size) {
                    data.read(buffer, block_size - read_size);
                    single_value.append(std::string(buffer, data.gcount()));
                    read_size += data.gcount();
                    break;
                }
                data.read(buffer, buffer_size);
                single_value.append(std::string(buffer, data.gcount()));
                read_size += data.gcount();
            }

            if (!data.eof()) {
                std::string new_line;
                std::getline(data, new_line, '\n');
                single_value.append(new_line);
            }
            value.push_back(single_value);
            data.close();
        }

        Storage::vector_put_string(table_name, cf_name, shard_id, row, column, value);
    }

    return 1;
}

int64_t find_local_file(std::vector<std::string>& files,
                        std::vector<int64_t>& sizes,
                        const std::string& path) {

    boost::filesystem::path dir_path(path);
    if (!boost::filesystem::exists( dir_path)){
        LOG(FATAL) << "CANNOT FIND PATH "
                   << path;
    }

    if (!boost::filesystem::is_directory(dir_path)) {
                LOG(FATAL) << path << " IS NOT DIR";
    }

    boost::filesystem::directory_iterator end_itr;

    for (boost::filesystem::directory_iterator itr(dir_path); itr != end_itr; ++itr) {

        if (boost::filesystem::is_directory(itr->status())) {
            find_local_file(files, sizes, itr->path().string());
        } else if (boost::filesystem::is_regular(itr->status())) {
            files.push_back(itr->path().string());
            sizes.push_back(boost::filesystem::file_size(itr->path()));
        }
    }
    return 1;
}

int64_t load_local_file_dir(const std::string& path,
                        const std::string& table_name,
                        const std::string& cf_name,
                        const int64_t block_size,
                        StorageType::type storage_type) {

    std::vector<std::string> txt_files;
    std::vector<int64_t> txt_sizes;
    find_local_file(txt_files, txt_sizes, path);

    load_local_file_regular(txt_files, txt_sizes, table_name, cf_name, block_size, storage_type);

    return 1;
}

int64_t DAG::load_local_file(const std::string& path,
                    const std::string& table_name,
                    const std::string& cf_name,
                    const int64_t block_size,
                    StorageType::type storage_type) {



    boost::filesystem::path check_path(path);
    if(boost::filesystem::is_directory(check_path)) {
        load_local_file_dir(path, table_name, cf_name, block_size, storage_type);
    } else if (boost::filesystem::is_regular(check_path)) {
        std::vector<std::string> _path;
        std::vector<int64_t> _size;
        _path.push_back(path);
        _size.push_back(boost::filesystem::file_size(check_path));
        load_local_file_regular(_path, _size, table_name, cf_name, block_size, storage_type);
    } else {
        LOG(FATAL) << path << "IS NOT A DIR OR A REGULAR FILE";
    }

    return 1;
}


}
}
