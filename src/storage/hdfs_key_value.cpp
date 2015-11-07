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

#ifdef HDFS_KEY_VALUE

#include <map>
#include "kv_base.h"
#include "../tools/include/hdfs/hdfs.h"

namespace ntu {
namespace cap {

template <class VALUE_T>
HdfsKeyValue<VALUE_T>::HdfsKeyValue():KVStorage() {
}

template <class VALUE_T>
HdfsKeyValue<VALUE_T>::HdfsKeyValue(const std::string table_name,
                       const int64_t shard_id,
                       const std::string cf_name):
                       KVStorage(table_name, shard_id, cf_name) {

    _builder = hdfsNewBuilder();
    hdfsBuilderSetForceNewInstance(_builder);
    hdfsBuilderSetNameNode(_builder, NodeInfo::singleton()._hdfs_namenode.c_str());
    hdfsBuilderSetNameNodePort(_builder, NodeInfo::singleton()._hdfs_namenode_port);

    std::string cf_property;
    cf_property.append(cf_name);
    cf_property.append("_hdfs_property");

    auto i =  NodeInfo::singleton()._client_task_tracker[NodeInfo::singleton()._node_id];
    i->open_transport();
    i->method()->scan_all_string(_property, table_name, shard_id, cf_property);
    i->close_transport();
}

template <class VALUE_T>
HdfsKeyValue<VALUE_T>::~HdfsKeyValue() {

}

template <class VALUE_T>
int64_t HdfsKeyValue<VALUE_T>::vector_put(std::vector<std::string> row_key,
                  std::vector<std::string> column_key,
                  std::vector<VALUE_T> value) {
   CHECK_EQ(row_key.size(), column_key.size());
   CHECK_EQ(row_key.size(), value.size());
   write_lock _lock(KVStorage::_rwmutex);
   LOG(FATAL) << "NOT IMPLEMENTED \n";
   return 1;
}

template <class VALUE_T>
int64_t HdfsKeyValue<VALUE_T>::vector_merge(std::vector<std::string> row_key,
                     std::vector<std::string> column_key,
                     std::vector<VALUE_T> value) {
      CHECK_EQ(row_key.size(), column_key.size());
      CHECK_EQ(row_key.size(), value.size());
      write_lock _lock(KVStorage::_rwmutex);
      LOG(FATAL) << "NOT IMPLEMENTED \n";
      return 1;
  }

template <class VALUE_T>
int64_t HdfsKeyValue<VALUE_T>::timed_vector_put(std::vector<std::string> row_key,
                         std::vector<std::string> column_key,
                         int64_t time_stamp,
                         std::vector<VALUE_T> value) {
   CHECK_EQ(row_key.size(), column_key.size());
   CHECK_EQ(row_key.size(), value.size());
   write_lock _lock(KVStorage::_rwmutex);
   LOG(FATAL) << "NOT IMPLEMENTED \n";
   return 1;
}

template <class VALUE_T>
void HdfsKeyValue<VALUE_T>::vector_get(std::vector<std::string> row_key,
                               std::vector<std::string> column_key,
                               std::vector<VALUE_T>& value) {
   CHECK_EQ(row_key.size(), column_key.size());
   value.clear();

   read_lock _lock(KVStorage::_rwmutex);
   LOG(FATAL) << "NOT IMPLEMENTED \n";
}

template <class VALUE_T>
void HdfsKeyValue<VALUE_T>::scan_all(std::map<std::string, std::map<std::string, VALUE_T>>& value) {
    read_lock _lock(KVStorage::_rwmutex);
    value.clear();

    _fs = hdfsBuilderConnect(_builder);

    std::string single_value;
    int64_t buffer_size = 1024;
    char buffer[buffer_size];

    for (auto& file_hdfs : _property) {

        std::string file_name;
        file_name = file_hdfs.first;
        hdfsFile file = hdfsOpenFile(_fs, file_name.c_str(), O_RDONLY, 0, 0, 0);

        for (auto& block_hdfs : file_hdfs.second) {

            int shard_id = std::stoi (block_hdfs.first, nullptr, 10);
            int block_size = std::stoi (block_hdfs.second, nullptr, 10);
            single_value.clear();
            hdfsSeek(_fs, file, block_size * shard_id);

            if (shard_id != 0) {
               while (hdfsRead(_fs, file, buffer, 1)) {
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
                    each_read_size = hdfsRead(_fs, file, buffer, buffer_size);
                    single_value.append(std::string(buffer, each_read_size));
                    total_read_size += each_read_size;
                    break;
                }
                each_read_size = hdfsRead(_fs, file, buffer, buffer_size);
                if (each_read_size == 0) {
                    eof = true;
                    break;
                }
                single_value.append(std::string(buffer, each_read_size));
                total_read_size += each_read_size;
            }
            while (!eof) {
                memset(buffer, 0, buffer_size);
                each_read_size = hdfsRead(_fs, file, buffer, buffer_size);
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
            value[file_name][block_hdfs.first] = single_value;
        }

        hdfsCloseFile(_fs, file);
   }
   hdfsDisconnect(_fs);
}

template <class VALUE_T>
void HdfsKeyValue<VALUE_T>::timed_scan(int64_t time_stamp,
        std::map<std::string, std::map<std::string, VALUE_T>>& value) {
   read_lock _lock(KVStorage::_rwmutex);
   value.clear();
   LOG(FATAL) << "NOT IMPLEMENTED \n";
}

template <class VALUE_T>
struct hdfsBuilder* HdfsKeyValue<VALUE_T>::storage_ptr() {
   return _builder;
}

}
}

#endif
