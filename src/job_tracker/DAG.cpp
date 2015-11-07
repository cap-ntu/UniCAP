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

std::thread DAG::initial() {
    YAML::Node config = YAML::LoadFile("../etc/unicap.yaml");
    const std::string application_name = config["application_name"].as<std::string>();
    const std::string jobtracker_host = config["jobtracker_host"].as<std::string>();
    const int64_t jobtracker_port = config["jobtracker_port"].as<int64_t>();
    const std::string hdfs_namenode_host = config["hdfs_namenode_host"].as<std::string>();
    const int64_t hdfs_namenode_port = config["hdfs_namenode_port"].as<int64_t>();
    const std::string hdfs_output_dir = config["hdfs_output_dir"].as<std::string>();
    const int64_t jobtracker_threads = config["jobtracker_threads"].as<int64_t>();

    NodeInfo::singleton()._app_name = application_name;
    NodeInfo::singleton()._master_host_name = jobtracker_host;
    NodeInfo::singleton()._master_port = jobtracker_port;
    NodeInfo::singleton()._hdfs_namenode = hdfs_namenode_host;
    NodeInfo::singleton()._hdfs_namenode_port = hdfs_namenode_port;
    NodeInfo::singleton()._root_dir = hdfs_output_dir;

    LOG(INFO) << "APPLICATION NAME: "       << application_name;
    LOG(INFO) << "JOBTRACKER HOSTNAME: "    << jobtracker_host;
    LOG(INFO) << "JOBTRACKER PORT: "        << jobtracker_port;
    LOG(INFO) << "HDFS NAMENODE HOSTNAME: " << hdfs_namenode_host;
    LOG(INFO) << "HDFS NAMENODE PORT: "     << hdfs_namenode_port;
    LOG(INFO) << "HDFS OUTPUT DIR: "        << hdfs_output_dir;
    LOG(INFO) << "JOBTRACKER THREADS: "     << jobtracker_threads;

    return (DAG::start_job_tracker(jobtracker_threads));
}

std::thread DAG::start_job_tracker(int64_t thread_num) {

    JobTrackerServer::singleton().set_port(NodeInfo::singleton()._master_port);
    JobTrackerServer::singleton().set_thread_num(thread_num);
    auto server_thread = JobTrackerServer::singleton().start();

    while(NodeInfo::singleton()._ready_task_tracker_number
            != NodeInfo::singleton()._task_tracker_number
            || NodeInfo::singleton()._ready_task_tracker_number == 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    JobTrackerServer::singleton().create_task_tracker_client();
    JobTrackerServer::singleton().check_client_task_tracker();

    return server_thread;
}

int64_t DAG::create_table(const std::string& table_name,
                         const int64_t shard_num) {

    KeyPartition table_partition;
    table_partition.__set_partition_algo(KeyPartitionAlgo::HashingPartition);
    create_table(table_name, shard_num, table_partition);
    return 1;
}

int64_t DAG::create_table(const std::string& table_name,
                     const int64_t shard_num,
                     const KeyPartition& partition) {

    Table new_table(table_name, shard_num, partition);
    new_table.allocate_shard();

    if (StorageInfo::singleton()._table_info.find(table_name)
            != StorageInfo::singleton()._table_info.end()) {
        LOG(FATAL) << "TABLE ALREADY EXISTS !";
    }

    StorageInfo::singleton()._table_info[table_name] = new_table;

    for (auto i : NodeInfo::singleton()._client_task_tracker) {
        i.second->open_transport();
        i.second->method()->create_table(new_table._table_property);
        i.second->close_transport();
    }

    DLOG(INFO) << "CREATE TABLE " << table_name;
    return 1;
}

int64_t DAG::create_table(const std::string& table_name,
                     const Table& base_table) {

    Table new_table(table_name, base_table);

    if (StorageInfo::singleton()._table_info.find(table_name)
            != StorageInfo::singleton()._table_info.end()) {
        LOG(FATAL) << "TABLE ALREADY EXISTS !";
    }

    StorageInfo::singleton()._table_info[table_name] = new_table;

    for (auto i : NodeInfo::singleton()._client_task_tracker) {
        i.second->open_transport();
        i.second->method()->create_table(new_table._table_property);
        i.second->close_transport();
    }

    DLOG(INFO) << "CREATE TABLE " << table_name;
    return 1;
}

int64_t DAG::create_cf(const std::string& table_name,
                  const std::string& cf_name,
                  const StorageType::type cf_type,
                  const ValueType::type value_type,
                  const std::pair<int64_t, int64_t> size) {

    ColumnFamily new_cf(cf_name, cf_type);
    new_cf._cf_property.block_size.resize(2);
    new_cf._cf_property.block_size[0] = size.first;
    new_cf._cf_property.block_size[1] = size.second;
    new_cf._cf_property.value_type = value_type;

    for (auto& exist_cf : StorageInfo::singleton()._cf_info[table_name]) {
        if (exist_cf.first == cf_name) {
            LOG(FATAL) << "CF ALREADY EXISTS !";
        }
    }

    StorageInfo::singleton()._cf_info[table_name][cf_name] = new_cf;

    for (auto i : NodeInfo::singleton()._client_task_tracker) {
        i.second->open_transport();
        CHECK_EQ (i.second->method()->create_cf(table_name, new_cf._cf_property), 1);
        i.second->close_transport();
    }

    DLOG(INFO) << "CREATE CF " << cf_name << " FOR TABLE " << table_name;
    return 1;
}

int64_t DAG::create_distributed_cache(const std::string& table_name,
                                    const std::string& cf_name,
                                    const std::string& cached_table_name,
                                    const std::string& cached_cf_name,
                                    const StorageType::type cf_type) {

    KeyPartition distributed_cache_partition;
    distributed_cache_partition.__set_partition_algo(KeyPartitionAlgo::NoneAlgo);
    Table new_table(table_name,
                    NodeInfo::singleton()._task_tracker_number,
                    distributed_cache_partition);
    new_table.replicate_shard();

    if (StorageInfo::singleton()._table_info.find(table_name)
            != StorageInfo::singleton()._table_info.end()) {
        LOG(FATAL) << "TABLE ALREADY EXISTS !";
    }

    StorageInfo::singleton()._table_info[table_name] = new_table;

    for (auto i : NodeInfo::singleton()._client_task_tracker) {
        i.second->open_transport();
        i.second->method()->create_table(new_table._table_property);
        i.second->close_transport();
    }

    DLOG(INFO) << "CREATE TABLE " << table_name;

    auto value_type = StorageInfo::singleton()._cf_info[table_name][cf_name]._cf_property.value_type;
    create_cf(table_name, cf_name, cf_type, value_type);

    std::shared_ptr<Stage>load_distributed_cache = std::shared_ptr<Stage>(new Stage());
    load_distributed_cache->set_function_name("load_distributed_cache");
    std::vector<std::string> src_cf;
    src_cf.push_back(cf_name);
    load_distributed_cache->set_src(table_name, src_cf);
    load_distributed_cache->set_dst(cached_table_name, cached_cf_name);

    int64_t stage_id = Scheduler::singleton().push_back(load_distributed_cache);

    while(!Scheduler::singleton().check_status(stage_id)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    return 1;
}

}
}


