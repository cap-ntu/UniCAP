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

int64_t load_img_regular(std::vector<std::string>& path,
                            std::vector<int64_t>& size,
                            const std::string& table_name,
                            const std::string& cf_name,
                            const int64_t block_size,
                            std::vector<std::vector<std::string>>& full_chuncks) {
   // std::vector<std::pair<std::string, int64_t>> free_chuncks;
    CHECK_EQ(path.size(), size.size());
    std::vector<std::pair<std::string, int64_t>> new_node;

    std::vector<uint64_t> merged_id;
    std::vector<std::string> new_chunck;
    int64_t first_ck_size = 0;
    int64_t target_ck_size = 0;
    int64_t initial_size = path.size();
    int64_t merged_size = 0;

    while(initial_size != merged_size) {
        merged_id.clear();
        new_chunck.clear();
        first_ck_size = size[0];
        target_ck_size = 0;
        merged_id.push_back(0);

        for (int64_t i = 1; i < initial_size - merged_size; ++i) {
            target_ck_size = size[i];
            if ((first_ck_size + target_ck_size) < (1.0 * block_size)) {
                merged_id.push_back(i);
                first_ck_size += target_ck_size;
            } else {
                break;
            }
        }
        new_chunck.insert(new_chunck.begin(), path.begin(), path.begin() + merged_id.size());
        merged_size += merged_id.size();

        path.erase(path.begin(), path.begin() + merged_id.size());
        size.erase(size.begin(), size.begin() + merged_id.size());

        full_chuncks.push_back(new_chunck);
    }
    return 1;
}

int64_t load_local_img_regular(std::vector<std::string>& path,
                    std::vector<int64_t>& size,
                    const std::string& table_name,
                    const std::string& cf_name,
                    const int64_t block_size) {

    std::vector<std::vector<std::string>> chuncks;

    load_img_regular(path, size, table_name, cf_name, block_size, chuncks);

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
    DAG::create_cf(table_name, cf_name, StorageType::InMemoryImage, ValueType::type::String);
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
            std::ifstream data(i);
            row.push_back(i);
            column.push_back("");
            while (!data.eof()) {
                memset(buffer, 0, buffer_size);
                data.read(buffer, buffer_size);
                single_value.append(std::string(buffer, data.gcount()));
            }
            value.push_back(single_value);
            data.close();
        }
        Storage::vector_put_string(table_name, cf_name, shard_id, row, column, value);
    }

    return 1;
}

int64_t find_local_img(std::vector<std::string>& imgs,
                        std::vector<int64_t>& sizes,
                        const std::string& path,
                        const int64_t block_size) {

    boost::filesystem::path dir_path(path);
    if (!boost::filesystem::exists( dir_path)){
        LOG(FATAL) << "CANNOT FIND PATH "
                   << path;
    }

    if (!boost::filesystem::is_directory(dir_path)) {
                LOG(FATAL) << path << " IS NOT DIR";
    }

    boost::filesystem::directory_iterator end_itr;
    int64_t size = 0;
    std::string img_name;
    for (boost::filesystem::directory_iterator itr(dir_path); itr != end_itr; ++itr) {

        if (boost::filesystem::is_directory(itr->status())) {
            find_local_img(imgs, sizes, itr->path().string(), block_size);
        } else if (boost::filesystem::is_regular(itr->status())) {
            size = boost::filesystem::file_size(itr->path());
            img_name = itr->path().string();

            if (size <= block_size &&
                    ((img_name.find(".JPEG") == img_name.size()-5) ||
                     (img_name.find(".jpeg") == img_name.size()-5) ||
                     (img_name.find(".JPG")  == img_name.size()-4) ||
                     (img_name.find(".jpg")  == img_name.size()-4) ||
                     (img_name.find(".TIFF") == img_name.size()-5) ||
                     (img_name.find(".tiff") == img_name.size()-5) ||
                     (img_name.find(".TIF")  == img_name.size()-4) ||
                     (img_name.find(".tif")  == img_name.size()-4) ||
                     (img_name.find(".PNG")  == img_name.size()-4) ||
                     (img_name.find(".png")  == img_name.size()-4) ||
                     (img_name.find(".BMP")  == img_name.size()-4) ||
                     (img_name.find(".bmp")  == img_name.size()-4) )) {

                imgs.push_back(itr->path().string());
                sizes.push_back(size);
            }
        }
    }
    return 1;
}

int64_t load_local_img_dir(const std::string& path,
                        const std::string& table_name,
                        const std::string& cf_name,
                        const int64_t block_size) {

    std::vector<std::string> imgs;
    std::vector<int64_t> img_sizes;
    find_local_img(imgs, img_sizes, path, block_size);

    load_local_img_regular(imgs, img_sizes, table_name, cf_name, block_size);

    return 1;
}

int64_t DAG::load_local_img(const std::string& path,
                        const std::string& table_name,
                        const std::string& cf_name,
                        const int64_t block_size) {
    boost::filesystem::path check_path(path);
    if(boost::filesystem::is_directory(check_path)) {
        load_local_img_dir(path, table_name, cf_name, block_size);
    } else if (boost::filesystem::is_regular(check_path)) {
        std::vector<std::string> _path;
        std::vector<int64_t> _size;
        _path.push_back(path);
        _size.push_back(boost::filesystem::file_size(check_path));
        load_local_img_regular(_path, _size, table_name, cf_name, block_size);
    } else {
        LOG(FATAL) << path << "IS NOT A DIR OR A REGULAR FILE";
    }

   return 1;
}


int64_t find_hdfs_img(std::vector<std::string>& imgs,
                    std::vector<int64_t>& sizes,
                    const std::string& path,
                    const int64_t block_size,
                    hdfsFS fs) {
    hdfsFileInfo *check_path;
    check_path = hdfsGetPathInfo(fs, path.c_str());
    std::string img_name = path;

    if (check_path->mKind == tObjectKind::kObjectKindFile) {
        if (check_path->mSize <= block_size &&
                ((img_name.find(".JPEG") == img_name.size()-5) ||
                 (img_name.find(".jpeg") == img_name.size()-5) ||
                 (img_name.find(".JPG")  == img_name.size()-4) ||
                 (img_name.find(".jpg")  == img_name.size()-4) ||
                 (img_name.find(".TIFF") == img_name.size()-5) ||
                 (img_name.find(".tiff") == img_name.size()-5) ||
                 (img_name.find(".TIF")  == img_name.size()-4) ||
                 (img_name.find(".tif")  == img_name.size()-4) ||
                 (img_name.find(".PNG")  == img_name.size()-4) ||
                 (img_name.find(".png")  == img_name.size()-4) ||
                 (img_name.find(".BMP")  == img_name.size()-4) ||
                 (img_name.find(".bmp")  == img_name.size()-4) )) {

            imgs.push_back(path);
            sizes.push_back(check_path->mSize);
            }
    } else if (check_path->mKind == tObjectKind::kObjectKindDirectory) {
        hdfsFileInfo *check_dir;
        int numbers;
        check_dir = hdfsListDirectory(fs, path.c_str(), &numbers);
        for (int i = 0; i < numbers; ++i) {
            if (check_dir->mKind == tObjectKind::kObjectKindFile) {
                img_name = check_dir->mName;
                if (check_dir->mSize > block_size) {
                    continue;
                }
                if (check_path->mSize <= block_size &&
                        ((img_name.find(".JPEG") == img_name.size()-5) ||
                         (img_name.find(".jpeg") == img_name.size()-5) ||
                         (img_name.find(".JPG")  == img_name.size()-4) ||
                         (img_name.find(".jpg")  == img_name.size()-4) ||
                         (img_name.find(".TIFF") == img_name.size()-5) ||
                         (img_name.find(".tiff") == img_name.size()-5) ||
                         (img_name.find(".TIF")  == img_name.size()-4) ||
                         (img_name.find(".tif")  == img_name.size()-4) ||
                         (img_name.find(".PNG")  == img_name.size()-4) ||
                         (img_name.find(".png")  == img_name.size()-4) ||
                         (img_name.find(".BMP")  == img_name.size()-4) ||
                         (img_name.find(".bmp")  == img_name.size()-4) )) {

                    imgs.push_back(check_dir->mName);
                    sizes.push_back(check_dir->mSize);
                }
            } else if (check_dir->mKind == tObjectKind::kObjectKindDirectory) {
                find_hdfs_img(imgs, sizes, check_dir->mName, block_size, fs);
            }
            ++check_dir;
        }
    } else {
        LOG(FATAL) << "CANNOT DETECT HDFS FILE TYPE";
    }
    return 1;
}

int64_t load_hdfs_img_regular(std::vector<std::string>& path,
                            std::vector<int64_t>& size,
                            const std::string& table_name,
                            const std::string& cf_name,
                            const int64_t block_size,
                            hdfsFS fs) {

    std::vector<std::vector<std::string>> chuncks;

    load_img_regular(path, size, table_name, cf_name, block_size, chuncks);

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

            row.push_back(i);
            column.push_back("");
            value.push_back("");
        }
        Storage::vector_put_string(table_name, cf_property, shard_id, row, column, value);
    }

    DAG::create_cf(table_name, cf_name, StorageType::type::InMemoryImage, ValueType::type::String);

    std::shared_ptr<Stage>stage_load_hdfs = std::shared_ptr<Stage>(new Stage());
    stage_load_hdfs->set_function_name("load_hdfs_image");
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


int64_t load_hdfs_img_dir(const std::string& path,
                            const std::string& table_name,
                            const std::string& cf_name,
                            const int64_t block_size,
                            hdfsFS fs) {
    std::vector<std::string> imgs;
    std::vector<int64_t> sizes;
    find_hdfs_img(imgs, sizes, path, block_size, fs);
    load_hdfs_img_regular(imgs, sizes, table_name, cf_name, block_size, fs);
    return 1;
}

int64_t DAG::load_hdfs_img(const std::string& path,
                    const std::string& table_name,
                    const std::string& cf_name,
                    const int64_t block_size) {

    struct hdfsBuilder *builder = hdfsNewBuilder();
    hdfsBuilderSetNameNode(builder, NodeInfo::singleton()._hdfs_namenode.c_str());
    hdfsBuilderSetNameNodePort(builder, NodeInfo::singleton()._hdfs_namenode_port);
    hdfsFS fs = hdfsBuilderConnect(builder);

    LOG(INFO) << "CONNECTING HDFS";

    if (hdfsExists(fs, path.c_str()) == -1) {
        LOG(FATAL) << "CANNOT FIND HDFS PATH " << path;
    }

    hdfsFileInfo *check_path;
    check_path = hdfsGetPathInfo(fs, path.c_str());

    LOG(INFO) << "COMPLETE CONNECT HDFS";

if (check_path->mKind == tObjectKind::kObjectKindFile) {
        std::vector<std::string> regular_path;
        std::vector<int64_t> regular_size;
         regular_path.push_back(path);
         regular_size.push_back(check_path->mSize);
         load_hdfs_img_regular(regular_path,
                             regular_size,
                             table_name,
                             cf_name,
                             block_size,
                             fs);
    } else if (check_path->mKind == tObjectKind::kObjectKindDirectory) {
        load_hdfs_img_dir(path, table_name, cf_name, block_size, fs);
    } else {
        LOG(FATAL) << "CANNOT DETECT HDFS FILE TYPE";
    }

    hdfsDisconnect(fs);

    return 1;
}

}
}


