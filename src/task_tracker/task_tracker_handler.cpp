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
#include "./task_tracker_handler.h"

namespace ntu {
namespace cap {


TaskTrackerHandler::TaskTrackerHandler() {
    // Your initialization goes here
}

void TaskTrackerHandler::ping(std::string& _return) {
    _return = "Pong";
}

int64_t TaskTrackerHandler::check_table(const std::string& table_name,
                                        const int64_t shard_id,
                                        const std::string& cf_name) {
    if (StorageInfo::singleton()._cf_ptr.find(table_name)
            == StorageInfo::singleton()._cf_ptr.end() ) {
        LOG(FATAL) << "CANNOT FIND TABLE " << table_name;
    }

    if (StorageInfo::singleton()._cf_ptr[table_name].find(shard_id)
            == StorageInfo::singleton()._cf_ptr[table_name].end() ) {
        LOG(FATAL) << "CANNOT FIND SHARD ID " << shard_id;
    }

    if (StorageInfo::singleton()._cf_ptr[table_name][shard_id].find(cf_name)
            == StorageInfo::singleton()._cf_ptr[table_name][shard_id].end() ) {
        LOG(FATAL) << "CANNOT FIND SHARD CF " << cf_name;
    }

    return 1;
}

int64_t TaskTrackerHandler::create_table(const TableProperty& table_property) {
    // Your implementation goes here
    Table new_table(table_property);
    StorageInfo::singleton()._table_info[table_property.table_name] = new_table;

    DLOG(INFO) << NodeInfo::singleton()._node_id
               << "("
               << NodeInfo::singleton()._host_name
               << ":"
               << NodeInfo::singleton()._port
               << ")->"
               << "CREATE TABLE "
               << new_table._table_property.table_name;
    return 1;
}

int64_t TaskTrackerHandler::create_cf(const std::string& table_name,
                                      const ColumnFamilyProperty& cf_property) {
    // Your implementation goes here
    if (StorageInfo::singleton()._table_info.find(table_name)
            == StorageInfo::singleton()._table_info.end()) {
        LOG(FATAL) << "CANNOT CREATE CF FOR UNEXIST TABLE " << table_name;
    }

    StorageInfo::singleton()._cf_info[table_name][cf_property.cf_name] = cf_property;

    int64_t node_id = NodeInfo::singleton()._node_id;

    switch (cf_property.storage_type) {

    case StorageType::type::InMemoryKeyValue: {
        //table_name -> shard_id -> cf_name -> ptr
        if (StorageInfo::singleton()._table_info[table_name].
                _table_property.node_info.find(node_id) !=
            StorageInfo::singleton()._table_info[table_name].
                            _table_property.node_info.end()) {
            for (int64_t i : StorageInfo::singleton()._table_info[table_name].
                    _table_property.node_info[node_id]) {
                if (cf_property.value_type == ValueType::type::Int64) {
                    StorageInfo::singleton()._cf_ptr[table_name][i][cf_property.cf_name]
                        = std::make_shared<InMemoryKeyValue<int64_t>>(table_name, i, cf_property.cf_name);
                } else if (cf_property.value_type == ValueType::type::Double) {
                    StorageInfo::singleton()._cf_ptr[table_name][i][cf_property.cf_name]
                        = std::make_shared<InMemoryKeyValue<double>>(table_name, i, cf_property.cf_name);
                } else if (cf_property.value_type == ValueType::type::String) {
                    StorageInfo::singleton()._cf_ptr[table_name][i][cf_property.cf_name]
                        = std::make_shared<InMemoryKeyValue<std::string>>(table_name, i, cf_property.cf_name);
                }
            }
        }
        break;
    }

    case StorageType::type::LSMKeyValue: {
        //table_name -> shard_id -> cf_name -> ptr
        if (StorageInfo::singleton()._table_info[table_name].
                        _table_property.node_info.find(node_id) !=
            StorageInfo::singleton()._table_info[table_name].
                            _table_property.node_info.end()) {
            for (int64_t i : StorageInfo::singleton()._table_info[table_name].
                    _table_property.node_info[node_id]) {
                if (cf_property.value_type == ValueType::type::Int64) {
                    StorageInfo::singleton()._cf_ptr[table_name][i][cf_property.cf_name]
                        = std::make_shared<LSMKeyValue<int64_t>>(table_name, i, cf_property.cf_name);
                } else if (cf_property.value_type == ValueType::type::Double) {
                    StorageInfo::singleton()._cf_ptr[table_name][i][cf_property.cf_name]
                        = std::make_shared<LSMKeyValue<double>>(table_name, i, cf_property.cf_name);
                } else if (cf_property.value_type == ValueType::type::String) {
                    StorageInfo::singleton()._cf_ptr[table_name][i][cf_property.cf_name]
                        = std::make_shared<LSMKeyValue<std::string>>(table_name, i, cf_property.cf_name);
                }
            }
        }
        break;
    }
    case StorageType::type::HdfsKeyValue: {
        //table_name -> shard_id -> cf_name -> ptr
        if (StorageInfo::singleton()._table_info[table_name].
                        _table_property.node_info.find(node_id) !=
            StorageInfo::singleton()._table_info[table_name].
                            _table_property.node_info.end()) {
            for (int64_t i : StorageInfo::singleton()._table_info[table_name].
                    _table_property.node_info[node_id]) {
                if (cf_property.value_type == ValueType::type::Int64) {
                    LOG(ERROR) << "HDFS ONLY SUPPORT STRING VALUE";
                } else if (cf_property.value_type == ValueType::type::Double) {
                    LOG(ERROR) << "HDFS ONLY SUPPORT STRING VALUE";
                } else if (cf_property.value_type == ValueType::type::String) {
                   StorageInfo::singleton()._cf_ptr[table_name][i][cf_property.cf_name]
                       = std::make_shared<HdfsKeyValue<std::string>>(table_name, i, cf_property.cf_name);
                }
            }
        }
        break;
    }

    case StorageType::type::InMemoryImage: {
        //table_name -> shard_id -> cf_name -> ptr
        if (StorageInfo::singleton()._table_info[table_name].
                        _table_property.node_info.find(node_id) !=
            StorageInfo::singleton()._table_info[table_name].
                            _table_property.node_info.end()) {
            for (int64_t i : StorageInfo::singleton()._table_info[table_name].
                    _table_property.node_info[node_id]) {
                if (cf_property.value_type == ValueType::type::Int64) {
                    LOG(ERROR) << "IMAGE STORE ONLY SUPPORT STRING VALUE";
                } else if (cf_property.value_type == ValueType::type::Double) {
                    LOG(ERROR) << "IMAGE STORE ONLY SUPPORT STRING VALUE";
                } else if (cf_property.value_type == ValueType::type::String) {
                    StorageInfo::singleton()._cf_ptr[table_name][i][cf_property.cf_name]
                      = std::make_shared<InMemoryImage<std::string>>(table_name, i, cf_property.cf_name);
                }
            }
        }
        break;
    }

    case StorageType::type::DenseMatrix: {
        //table_name -> shard_id -> cf_name -> ptr
        if (StorageInfo::singleton()._table_info[table_name].
                        _table_property.node_info.find(node_id) !=
            StorageInfo::singleton()._table_info[table_name].
                            _table_property.node_info.end()) {
            for (int64_t i : StorageInfo::singleton()._table_info[table_name].
                    _table_property.node_info[node_id]) {
                if (cf_property.value_type == ValueType::type::Int64) {
                    StorageInfo::singleton()._cf_ptr[table_name][i][cf_property.cf_name]
                      = std::make_shared<DenseMatrix<int64_t>>(table_name, i, cf_property.cf_name,
                              std::make_pair(cf_property.block_size[0], cf_property.block_size[1]));
                } else if (cf_property.value_type == ValueType::type::Double) {
                    StorageInfo::singleton()._cf_ptr[table_name][i][cf_property.cf_name]
                      = std::make_shared<DenseMatrix<double>>(table_name, i, cf_property.cf_name,
                              std::make_pair(cf_property.block_size[0], cf_property.block_size[1]));
                } else if (cf_property.value_type == ValueType::type::String) {
                    LOG(ERROR) << "MATRIX ONLY SUPPORT INT AND DOUBLE";
                }
            }
        }
        break;
    }

    case StorageType::type::SparseMatrix: {
        //table_name -> shard_id -> cf_name -> ptr
        if (StorageInfo::singleton()._table_info[table_name].
                        _table_property.node_info.find(node_id) !=
            StorageInfo::singleton()._table_info[table_name].
                            _table_property.node_info.end()) {
            for (int64_t i : StorageInfo::singleton()._table_info[table_name].
                    _table_property.node_info[node_id]) {
                if (cf_property.value_type == ValueType::type::Int64) {
                    LOG(ERROR) << "SPARSE MATRIX ONLY SUPPORT DOUBLE";
                } else if (cf_property.value_type == ValueType::type::Double) {
                    StorageInfo::singleton()._cf_ptr[table_name][i][cf_property.cf_name]
                      = std::make_shared<SparseMatrix<double>>(table_name, i, cf_property.cf_name,
                              std::make_pair(cf_property.block_size[0], cf_property.block_size[1]));
                } else if (cf_property.value_type == ValueType::type::String) {
                    LOG(ERROR) << "MATRIX ONLY SUPPORT INT AND DOUBLE";
                }
            }
        }
        break;
    }

    default:
        break;
    }

    DLOG(INFO) << NodeInfo::singleton()._node_id
               << "("
               << NodeInfo::singleton()._host_name
               << ":"
               << NodeInfo::singleton()._port
               << ")->"
               << "CREATE CF "
               << table_name
               << " # "
               << cf_property.cf_name;
    return 1;
}

int64_t TaskTrackerHandler::vector_put_int(const std::string& table_name,
        const int64_t shard_id,
        const std::string& cf_name,
        const std::vector<std::string> & row_key,
        const std::vector<std::string> & column_key,
        const std::vector<int64_t> & value) {

    check_table(table_name, shard_id, cf_name);

    auto storage_ptr = StorageInfo::singleton()._cf_ptr[table_name][shard_id][cf_name];
    auto storage_type = StorageInfo::singleton()._cf_info[table_name][cf_name]._cf_property.storage_type;
    //auto value_type = StorageInfo::singleton()._cf_info[table_name][cf_name]._cf_property.value_type;

    switch (storage_type) {

    case StorageType::type::InMemoryKeyValue:{

        auto caster_ptr = std::static_pointer_cast<InMemoryKeyValue<int64>>(storage_ptr);
        caster_ptr->vector_put(row_key, column_key, value);
        break;
    }
    case StorageType::type::LSMKeyValue: {

        auto caster_ptr = std::static_pointer_cast<LSMKeyValue<int64>>(storage_ptr);
        caster_ptr->vector_put(row_key, column_key, value);
        break;
    }
    case StorageType::type::HdfsKeyValue: {

        LOG(ERROR) << "HDFS ONLY SUPPORT STRING";
        break;
    }
    case StorageType::type::InMemoryImage: {

        LOG(ERROR) << "InMemoryImage ONLY SUPPORT STRING";
        break;
    }
    case StorageType::type::DenseMatrix: {

        auto caster_ptr = std::static_pointer_cast<DenseMatrix<int64>>(storage_ptr);
        caster_ptr->vector_put(row_key, column_key, value);
        break;
    }
    case StorageType::type::SparseMatrix: {

        LOG(ERROR) << "SPARSE MATRIX ONLY SUPPORT DOUBLE";
        break;
    }

    default: {
        break;
    }
    }
    return 1;
}

int64_t TaskTrackerHandler::vector_put_double(const std::string& table_name,
        const int64_t shard_id, const std::string& cf_name,
        const std::vector<std::string> & row_key,
        const std::vector<std::string> & column_key,
        const std::vector<double> & value){

    check_table(table_name, shard_id, cf_name);

    auto storage_ptr = StorageInfo::singleton()._cf_ptr[table_name][shard_id][cf_name];
    auto storage_type = StorageInfo::singleton()._cf_info[table_name][cf_name]._cf_property.storage_type;
   // auto value_type = StorageInfo::singleton()._cf_info[table_name][cf_name]._cf_property.value_type;

    switch (storage_type) {

    case StorageType::type::InMemoryKeyValue:{

        auto caster_ptr = std::static_pointer_cast<InMemoryKeyValue<double>>(storage_ptr);
        caster_ptr->vector_put(row_key, column_key, value);
        break;
    }
    case StorageType::type::LSMKeyValue: {

        auto caster_ptr = std::static_pointer_cast<LSMKeyValue<double>>(storage_ptr);
        caster_ptr->vector_put(row_key, column_key, value);
        break;
    }
    case StorageType::type::HdfsKeyValue: {

        LOG(ERROR) << "HdfsKeyValue ONLY SUPPORT STRING";
        break;
    }
    case StorageType::type::InMemoryImage: {

        LOG(ERROR) << "InMemoryImage ONLY SUPPORT STRING";
        break;
    }
    case StorageType::type::DenseMatrix: {

        auto caster_ptr = std::static_pointer_cast<DenseMatrix<double>>(storage_ptr);
        caster_ptr->vector_put(row_key, column_key, value);
        break;
    }
    case StorageType::type::SparseMatrix: {

        auto caster_ptr = std::static_pointer_cast<SparseMatrix<double>>(storage_ptr);
        caster_ptr->vector_put(row_key, column_key, value);
        break;
    }

    default: {
        break;
    }
    }
    return 1;
}

int64_t TaskTrackerHandler::vector_put_string(const std::string& table_name,
        const int64_t shard_id,
        const std::string& cf_name,
        const std::vector<std::string> & row_key,
        const std::vector<std::string> & column_key,
        const std::vector<std::string> & value) {

    check_table(table_name, shard_id, cf_name);

    auto storage_ptr = StorageInfo::singleton()._cf_ptr[table_name][shard_id][cf_name];
    auto storage_type = StorageInfo::singleton()._cf_info[table_name][cf_name]._cf_property.storage_type;
   // auto value_type = StorageInfo::singleton()._cf_info[table_name][cf_name]._cf_property.value_type;

    switch (storage_type) {

    case StorageType::type::InMemoryKeyValue:{

        auto caster_ptr = std::static_pointer_cast<InMemoryKeyValue<std::string>>(storage_ptr);
        caster_ptr->vector_put(row_key, column_key, value);
        break;
    }
    case StorageType::type::LSMKeyValue: {

        auto caster_ptr = std::static_pointer_cast<LSMKeyValue<std::string>>(storage_ptr);
        caster_ptr->vector_put(row_key, column_key, value);
        break;
    }
    case StorageType::type::HdfsKeyValue: {

        auto caster_ptr = std::static_pointer_cast<HdfsKeyValue<std::string>>(storage_ptr);
        caster_ptr->vector_put(row_key, column_key, value);
        break;
    }
    case StorageType::type::InMemoryImage: {

        auto caster_ptr = std::static_pointer_cast<InMemoryImage<std::string>>(storage_ptr);
        caster_ptr->vector_put(row_key, column_key, value);
        break;
    }
    case StorageType::type::DenseMatrix: {

        LOG(ERROR) << "MATRIX ONLY SUPPORT NUM";
        break;
    }
    case StorageType::type::SparseMatrix: {

        LOG(ERROR) << "MATRIX ONLY SUPPORT NUM";
        break;
    }

    default: {
        break;
    }
    }
    return 1;
}

int64_t TaskTrackerHandler::vector_merge_int(const std::string& table_name,
        const int64_t shard_id,
        const std::string& cf_name,
        const std::vector<std::string> & row_key,
        const std::vector<std::string> & column_key,
        const std::vector<int64_t> & value) {

    check_table(table_name, shard_id, cf_name);

    auto storage_ptr = StorageInfo::singleton()._cf_ptr[table_name][shard_id][cf_name];
    auto storage_type = StorageInfo::singleton()._cf_info[table_name][cf_name]._cf_property.storage_type;
  //  auto value_type = StorageInfo::singleton()._cf_info[table_name][cf_name]._cf_property.value_type;

    switch (storage_type) {

    case StorageType::type::InMemoryKeyValue:{

        auto caster_ptr = std::static_pointer_cast<InMemoryKeyValue<int64>>(storage_ptr);
        caster_ptr->vector_merge(row_key, column_key, value);
        break;
    }
    case StorageType::type::LSMKeyValue: {

        auto caster_ptr = std::static_pointer_cast<LSMKeyValue<int64>>(storage_ptr);
        caster_ptr->vector_merge(row_key, column_key, value);
        break;
    }
    case StorageType::type::HdfsKeyValue: {

        LOG(ERROR) << "HDFS ONLY SUPPORT STRING";
        break;
    }
    case StorageType::type::InMemoryImage: {

        LOG(ERROR) << "InMemoryImage ONLY SUPPORT STRING";
        break;
    }
    case StorageType::type::DenseMatrix: {

        auto caster_ptr = std::static_pointer_cast<DenseMatrix<int64>>(storage_ptr);
        caster_ptr->vector_merge(row_key, column_key, value);
        break;
    }
    case StorageType::type::SparseMatrix: {

        LOG(ERROR) << "SPARSE MATRIX ONLY SUPPORT DOUBLE";
        break;
    }

    default: {
        break;
    }
    }
    return 1;
}

int64_t TaskTrackerHandler::vector_merge_double(const std::string& table_name,
        const int64_t shard_id, const std::string& cf_name,
        const std::vector<std::string> & row_key,
        const std::vector<std::string> & column_key,
        const std::vector<double> & value) {

    check_table(table_name, shard_id, cf_name);

    auto storage_ptr = StorageInfo::singleton()._cf_ptr[table_name][shard_id][cf_name];
    auto storage_type = StorageInfo::singleton()._cf_info[table_name][cf_name]._cf_property.storage_type;
   // auto value_type = StorageInfo::singleton()._cf_info[table_name][cf_name]._cf_property.value_type;

    switch (storage_type) {

    case StorageType::type::InMemoryKeyValue:{

        auto caster_ptr = std::static_pointer_cast<InMemoryKeyValue<double>>(storage_ptr);
        caster_ptr->vector_merge(row_key, column_key, value);
        break;
    }
    case StorageType::type::LSMKeyValue: {

        auto caster_ptr = std::static_pointer_cast<LSMKeyValue<double>>(storage_ptr);
        caster_ptr->vector_merge(row_key, column_key, value);
        break;
    }
    case StorageType::type::HdfsKeyValue: {

        LOG(ERROR) << "HDFS ONLY SUPPORT STRING";
        break;
    }
    case StorageType::type::InMemoryImage: {

        LOG(ERROR) << "InMemoryImage ONLY SUPPORT STRING";
        break;
    }
    case StorageType::type::DenseMatrix: {

        auto caster_ptr = std::static_pointer_cast<DenseMatrix<double>>(storage_ptr);
        caster_ptr->vector_merge(row_key, column_key, value);
        break;
    }
    case StorageType::type::SparseMatrix: {

        auto caster_ptr = std::static_pointer_cast<SparseMatrix<double>>(storage_ptr);
        caster_ptr->vector_merge(row_key, column_key, value);
        break;
    }

    default: {
        break;
    }
    }
    return 1;
}

int64_t TaskTrackerHandler::TaskTrackerHandler::vector_merge_string(const std::string& table_name,
        const int64_t shard_id,
        const std::string& cf_name,
        const std::vector<std::string> & row_key,
        const std::vector<std::string> & column_key,
        const std::vector<std::string> & value) {


    check_table(table_name, shard_id, cf_name);

    auto storage_ptr = StorageInfo::singleton()._cf_ptr[table_name][shard_id][cf_name];
    auto storage_type = StorageInfo::singleton()._cf_info[table_name][cf_name]._cf_property.storage_type;
   // auto value_type = StorageInfo::singleton()._cf_info[table_name][cf_name]._cf_property.value_type;

    switch (storage_type) {

    case StorageType::type::InMemoryKeyValue:{

        LOG(ERROR) << "STRING CANNOT BE MERGED";
        break;
    }
    case StorageType::type::LSMKeyValue: {

        LOG(ERROR) << "STRING CANNOT BE MERGED";
        break;
    }
    case StorageType::type::HdfsKeyValue: {

        LOG(ERROR) << "STRING CANNOT BE MERGED";
        break;
    }
    case StorageType::type::InMemoryImage: {

        LOG(ERROR) << "MATRIX ONLY SUPPORT NUM";
        break;
    }
    case StorageType::type::DenseMatrix: {

        LOG(ERROR) << "STRING CANNOT BE MERGED";
        break;
    }
    case StorageType::type::SparseMatrix: {

        LOG(ERROR) << "MATRIX ONLY SUPPORT NUM";
        break;
    }

    default: {
        break;
    }
    }
    return 1;
}

int64_t TaskTrackerHandler::timed_vector_put_int(const std::string& table_name,
        const int64_t shard_id,
        const std::string& cf_name,
        const std::vector<std::string> & row_key,
        const std::vector<std::string> & column_key,
        const int64_t time_stampe,
        const std::vector<int64_t> & value) {

    check_table(table_name, shard_id, cf_name);

    auto storage_ptr = StorageInfo::singleton()._cf_ptr[table_name][shard_id][cf_name];
    auto storage_type = StorageInfo::singleton()._cf_info[table_name][cf_name]._cf_property.storage_type;
  //  auto value_type = StorageInfo::singleton()._cf_info[table_name][cf_name]._cf_property.value_type;

    switch (storage_type) {

    case StorageType::type::InMemoryKeyValue:{

        auto caster_ptr = std::static_pointer_cast<InMemoryKeyValue<int64>>(storage_ptr);
        caster_ptr->timed_vector_put(row_key, column_key, time_stampe, value);
        break;
    }
    case StorageType::type::LSMKeyValue: {

        auto caster_ptr = std::static_pointer_cast<LSMKeyValue<int64>>(storage_ptr);
        caster_ptr->timed_vector_put(row_key, column_key, time_stampe, value);
        break;
    }
    case StorageType::type::HdfsKeyValue: {

        LOG(ERROR) << "HDFS ONLY SUPPORT STRING";
        break;
    }
    case StorageType::type::InMemoryImage: {

        LOG(ERROR) << "InMemoryImage ONLY SUPPORT STRING";
        break;
    }
    case StorageType::type::DenseMatrix: {

        auto caster_ptr = std::static_pointer_cast<DenseMatrix<int64>>(storage_ptr);
        caster_ptr->timed_vector_put(row_key, column_key, time_stampe, value);
        break;
    }
    case StorageType::type::SparseMatrix: {

        LOG(ERROR) << "SPARSE MATRIX ONLY SUPPORT DOUBLE";
        break;
    }

    default: {
        break;
    }
    }
    return 1;
}

int64_t TaskTrackerHandler::timed_vector_put_double(const std::string& table_name,
        const int64_t shard_id,
        const std::string& cf_name,
        const std::vector<std::string> & row_key,
        const std::vector<std::string> & column_key,
        const int64_t time_stampe,
        const std::vector<double> & value) {

    check_table(table_name, shard_id, cf_name);

    auto storage_ptr = StorageInfo::singleton()._cf_ptr[table_name][shard_id][cf_name];
    auto storage_type = StorageInfo::singleton()._cf_info[table_name][cf_name]._cf_property.storage_type;
  //  auto value_type = StorageInfo::singleton()._cf_info[table_name][cf_name]._cf_property.value_type;

    switch (storage_type) {

    case StorageType::type::InMemoryKeyValue:{

        auto caster_ptr = std::static_pointer_cast<InMemoryKeyValue<double>>(storage_ptr);
        caster_ptr->timed_vector_put(row_key, column_key, time_stampe, value);
        break;
    }
    case StorageType::type::LSMKeyValue: {

        auto caster_ptr = std::static_pointer_cast<LSMKeyValue<double>>(storage_ptr);
        caster_ptr->timed_vector_put(row_key, column_key, time_stampe, value);
        break;
    }
    case StorageType::type::HdfsKeyValue: {

        LOG(ERROR) << "HDFS ONLY SUPPORT STRING";
        break;
    }
    case StorageType::type::InMemoryImage: {

        LOG(ERROR) << "InMemoryImage ONLY SUPPORT STRING";
        break;
    }
    case StorageType::type::DenseMatrix: {

        auto caster_ptr = std::static_pointer_cast<DenseMatrix<double>>(storage_ptr);
        caster_ptr->timed_vector_put(row_key, column_key, time_stampe, value);
        break;
    }
    case StorageType::type::SparseMatrix: {

        auto caster_ptr = std::static_pointer_cast<SparseMatrix<double>>(storage_ptr);
        caster_ptr->timed_vector_put(row_key, column_key, time_stampe, value);
        break;
    }

    default: {
        break;
    }
    }
    return 1;
}

int64_t TaskTrackerHandler::timed_vector_put_string(const std::string& table_name,
        const int64_t shard_id,
        const std::string& cf_name,
        const std::vector<std::string> & row_key,
        const std::vector<std::string> & column_key,
        const int64_t time_stampe,
        const std::vector<std::string> & value) {

    check_table(table_name, shard_id, cf_name);

    auto storage_ptr = StorageInfo::singleton()._cf_ptr[table_name][shard_id][cf_name];
    auto storage_type = StorageInfo::singleton()._cf_info[table_name][cf_name]._cf_property.storage_type;
 //   auto value_type = StorageInfo::singleton()._cf_info[table_name][cf_name]._cf_property.value_type;

    switch (storage_type) {

    case StorageType::type::InMemoryKeyValue:{

        auto caster_ptr = std::static_pointer_cast<InMemoryKeyValue<std::string>>(storage_ptr);
        caster_ptr->timed_vector_put(row_key, column_key, time_stampe, value);
        break;
    }
    case StorageType::type::LSMKeyValue: {

        auto caster_ptr = std::static_pointer_cast<LSMKeyValue<std::string>>(storage_ptr);
        caster_ptr->timed_vector_put(row_key, column_key, time_stampe, value);
        break;
    }
    case StorageType::type::HdfsKeyValue: {

        auto caster_ptr = std::static_pointer_cast<HdfsKeyValue<std::string>>(storage_ptr);
        caster_ptr->timed_vector_put(row_key, column_key, time_stampe, value);
        break;
    }
    case StorageType::type::InMemoryImage: {

        auto caster_ptr = std::static_pointer_cast<InMemoryImage<std::string>>(storage_ptr);
        caster_ptr->timed_vector_put(row_key, column_key, time_stampe, value);
        break;
    }
    case StorageType::type::DenseMatrix: {

        LOG(ERROR) << "MATRIX ONLY SUPPORT NUM";
        break;
    }
    case StorageType::type::SparseMatrix: {

        LOG(ERROR) << "MATRIX ONLY SUPPORT NUM";
        break;
    }

    default: {
        break;
    }
    }
    return 1;
}

void TaskTrackerHandler::vector_get_int(std::vector<int64_t> & _return,
        const std::string& table_name,
        const int64_t shard_id,
        const std::string& cf_name,
        const std::vector<std::string> & row_key,
        const std::vector<std::string> & column_key) {

    check_table(table_name, shard_id, cf_name);

    auto storage_ptr = StorageInfo::singleton()._cf_ptr[table_name][shard_id][cf_name];
    auto storage_type = StorageInfo::singleton()._cf_info[table_name][cf_name]._cf_property.storage_type;
   // auto value_type = StorageInfo::singleton()._cf_info[table_name][cf_name]._cf_property.value_type;

    switch (storage_type) {

    case StorageType::type::InMemoryKeyValue:{

        auto caster_ptr = std::static_pointer_cast<InMemoryKeyValue<int64>>(storage_ptr);
        caster_ptr->vector_get(row_key, column_key, _return);
        break;
    }
    case StorageType::type::LSMKeyValue: {

        auto caster_ptr = std::static_pointer_cast<LSMKeyValue<int64>>(storage_ptr);
        caster_ptr->vector_get(row_key, column_key, _return);
        break;
    }
    case StorageType::type::HdfsKeyValue: {

        LOG(ERROR) << "HDFS ONLY SUPPORT STRING";;
        break;
    }
    case StorageType::type::InMemoryImage: {

        LOG(ERROR) << "InMemoryImage ONLY SUPPORT STRING";
        break;
    }
    case StorageType::type::DenseMatrix: {

        auto caster_ptr = std::static_pointer_cast<DenseMatrix<int64>>(storage_ptr);
        caster_ptr->vector_get(row_key, column_key, _return);
        break;
    }
    case StorageType::type::SparseMatrix: {

        LOG(ERROR) << "SPARSE MATRIX ONLY SUPPORT DOUBLE";
        break;
    }

    default: {
        break;
    }
    }
}

void TaskTrackerHandler::vector_get_double(std::vector<double> & _return,
        const std::string& table_name,
        const int64_t shard_id,
        const std::string& cf_name,
        const std::vector<std::string> & row_key,
        const std::vector<std::string> & column_key) {

    check_table(table_name, shard_id, cf_name);

    auto storage_ptr = StorageInfo::singleton()._cf_ptr[table_name][shard_id][cf_name];
    auto storage_type = StorageInfo::singleton()._cf_info[table_name][cf_name]._cf_property.storage_type;
 //   auto value_type = StorageInfo::singleton()._cf_info[table_name][cf_name]._cf_property.value_type;

    switch (storage_type) {

    case StorageType::type::InMemoryKeyValue:{

        auto caster_ptr = std::static_pointer_cast<InMemoryKeyValue<double>>(storage_ptr);
        caster_ptr->vector_get(row_key, column_key, _return);
        break;
    }
    case StorageType::type::LSMKeyValue: {

        auto caster_ptr = std::static_pointer_cast<LSMKeyValue<double>>(storage_ptr);
        caster_ptr->vector_get(row_key, column_key, _return);
        break;
    }
    case StorageType::type::HdfsKeyValue: {

        LOG(ERROR) << "HDFS ONLY SUPPORT STRING";
        break;
    }
    case StorageType::type::InMemoryImage: {

        LOG(ERROR) << "InMemoryImage ONLY SUPPORT STRING";
        break;
    }
    case StorageType::type::DenseMatrix: {

        auto caster_ptr = std::static_pointer_cast<DenseMatrix<double>>(storage_ptr);
        caster_ptr->vector_get(row_key, column_key, _return);
        break;
    }
    case StorageType::type::SparseMatrix: {

        auto caster_ptr = std::static_pointer_cast<SparseMatrix<double>>(storage_ptr);
        caster_ptr->vector_get(row_key, column_key, _return);
        break;
    }

    default: {
        break;
    }
    }
}

void TaskTrackerHandler::vector_get_string(std::vector<std::string> & _return,
        const std::string& table_name,
        const int64_t shard_id,
        const std::string& cf_name,
        const std::vector<std::string> & row_key,
        const std::vector<std::string> & column_key) {

    check_table(table_name, shard_id, cf_name);

    auto storage_ptr = StorageInfo::singleton()._cf_ptr[table_name][shard_id][cf_name];
    auto storage_type = StorageInfo::singleton()._cf_info[table_name][cf_name]._cf_property.storage_type;
    //auto value_type = StorageInfo::singleton()._cf_info[table_name][cf_name]._cf_property.value_type;

    switch (storage_type) {

    case StorageType::type::InMemoryKeyValue:{

        auto caster_ptr = std::static_pointer_cast<InMemoryKeyValue<std::string>>(storage_ptr);
        caster_ptr->vector_get(row_key, column_key, _return);
        break;
    }
    case StorageType::type::LSMKeyValue: {

        auto caster_ptr = std::static_pointer_cast<LSMKeyValue<std::string>>(storage_ptr);
        caster_ptr->vector_get(row_key, column_key, _return);
        break;
    }
    case StorageType::type::HdfsKeyValue: {

        auto caster_ptr = std::static_pointer_cast<HdfsKeyValue<std::string>>(storage_ptr);
        caster_ptr->vector_get(row_key, column_key, _return);
        break;
    }
    case StorageType::type::InMemoryImage: {

        auto caster_ptr = std::static_pointer_cast<InMemoryImage<std::string>>(storage_ptr);
        caster_ptr->vector_get(row_key, column_key, _return);
        break;
    }
    case StorageType::type::DenseMatrix: {

        LOG(ERROR) << "MATRIX ONLY SUPPORT NUM";
        break;
    }
    case StorageType::type::SparseMatrix: {

        LOG(ERROR) << "MATRIX ONLY SUPPORT NUM";
        break;
    }

    default: {
        break;
    }
    }
}

void TaskTrackerHandler::scan_all_int(std::map<std::string, std::map<std::string, int64_t> > & _return,
        const std::string& table_name,
        const int64_t shard_id,
        const std::string& cf_name) {

    check_table(table_name, shard_id, cf_name);

    auto storage_ptr = StorageInfo::singleton()._cf_ptr[table_name][shard_id][cf_name];
    auto storage_type = StorageInfo::singleton()._cf_info[table_name][cf_name]._cf_property.storage_type;
 //   auto value_type = StorageInfo::singleton()._cf_info[table_name][cf_name]._cf_property.value_type;

    switch (storage_type) {

    case StorageType::type::InMemoryKeyValue:{

        auto caster_ptr = std::static_pointer_cast<InMemoryKeyValue<int64>>(storage_ptr);
        caster_ptr->scan_all(_return);
        break;
    }
    case StorageType::type::LSMKeyValue: {

        auto caster_ptr = std::static_pointer_cast<LSMKeyValue<int64>>(storage_ptr);
        caster_ptr->scan_all(_return);
        break;
    }
    case StorageType::type::HdfsKeyValue: {

        LOG(ERROR) << "HDFS ONLY SUPPORT STRING";
        break;
    }
    case StorageType::type::InMemoryImage: {

        LOG(ERROR) << "InMemoryImage ONLY SUPPORT STRING";
        break;
    }
    case StorageType::type::DenseMatrix: {

        auto caster_ptr = std::static_pointer_cast<DenseMatrix<int64>>(storage_ptr);
        caster_ptr->scan_all(_return);
        break;
    }
    case StorageType::type::SparseMatrix: {

        LOG(ERROR) << "SPARSE MATRIX ONLY SUPPORT DOUBLE";
        break;
    }

    default: {
        break;
    }
    }
}

void TaskTrackerHandler::scan_all_double(std::map<std::string, std::map<std::string, double> > & _return,
        const std::string& table_name,
        const int64_t shard_id,
        const std::string& cf_name) {

    check_table(table_name, shard_id, cf_name);

    auto storage_ptr = StorageInfo::singleton()._cf_ptr[table_name][shard_id][cf_name];
    auto storage_type = StorageInfo::singleton()._cf_info[table_name][cf_name]._cf_property.storage_type;
//    auto value_type = StorageInfo::singleton()._cf_info[table_name][cf_name]._cf_property.value_type;

    switch (storage_type) {

    case StorageType::type::InMemoryKeyValue:{

        auto caster_ptr = std::static_pointer_cast<InMemoryKeyValue<double>>(storage_ptr);
        caster_ptr->scan_all(_return);
        break;
    }
    case StorageType::type::LSMKeyValue: {

        auto caster_ptr = std::static_pointer_cast<LSMKeyValue<double>>(storage_ptr);
        caster_ptr->scan_all(_return);
        break;
    }
    case StorageType::type::HdfsKeyValue: {

        LOG(ERROR) << "HDFS ONLY SUPPORT STRING";
        break;
    }
    case StorageType::type::InMemoryImage: {

        LOG(ERROR) << "InMemoryImage ONLY SUPPORT STRING";
        break;
    }
    case StorageType::type::DenseMatrix: {

        auto caster_ptr = std::static_pointer_cast<DenseMatrix<double>>(storage_ptr);
        caster_ptr->scan_all(_return);
        break;
    }
    case StorageType::type::SparseMatrix: {

        auto caster_ptr = std::static_pointer_cast<SparseMatrix<double>>(storage_ptr);
        caster_ptr->scan_all(_return);
        break;
    }

    default: {
        break;
    }
    }
}

void TaskTrackerHandler::scan_all_string(std::map<std::string, std::map<std::string, std::string> > & _return,
        const std::string& table_name,
        const int64_t shard_id,
        const std::string& cf_name) {

    check_table(table_name, shard_id, cf_name);

    auto storage_ptr = StorageInfo::singleton()._cf_ptr[table_name][shard_id][cf_name];
    auto storage_type = StorageInfo::singleton()._cf_info[table_name][cf_name]._cf_property.storage_type;
//    auto value_type = StorageInfo::singleton()._cf_info[table_name][cf_name]._cf_property.value_type;

    switch (storage_type) {

    case StorageType::type::InMemoryKeyValue:{

        auto caster_ptr = std::static_pointer_cast<InMemoryKeyValue<std::string>>(storage_ptr);
        caster_ptr->scan_all(_return);
        break;
    }
    case StorageType::type::LSMKeyValue: {

        auto caster_ptr = std::static_pointer_cast<LSMKeyValue<std::string>>(storage_ptr);
        caster_ptr->scan_all(_return);
        break;
    }
    case StorageType::type::HdfsKeyValue: {

        auto caster_ptr = std::static_pointer_cast<HdfsKeyValue<std::string>>(storage_ptr);
        caster_ptr->scan_all(_return);
        break;
    }
    case StorageType::type::InMemoryImage: {

        auto caster_ptr = std::static_pointer_cast<InMemoryImage<std::string>>(storage_ptr);
        caster_ptr->scan_all(_return);
        break;
    }
    case StorageType::type::DenseMatrix: {

        LOG(ERROR) << "MATRIX ONLY SUPPORT NUM";
        break;
    }
    case StorageType::type::SparseMatrix: {

        LOG(ERROR) << "MATRIX ONLY SUPPORT NUM";
        break;
    }

    default: {
        break;
    }
    }
}

void TaskTrackerHandler::timed_scan_int(std::map<std::string, std::map<std::string, int64_t> > & _return,
        const std::string& table_name,
        const int64_t shard_id,
        const std::string& cf_name,
        const int64_t time_stamp) {

    check_table(table_name, shard_id, cf_name);

    auto storage_ptr = StorageInfo::singleton()._cf_ptr[table_name][shard_id][cf_name];
    auto storage_type = StorageInfo::singleton()._cf_info[table_name][cf_name]._cf_property.storage_type;
 //   auto value_type = StorageInfo::singleton()._cf_info[table_name][cf_name]._cf_property.value_type;

    switch (storage_type) {

    case StorageType::type::InMemoryKeyValue:{

        auto caster_ptr = std::static_pointer_cast<InMemoryKeyValue<int64_t>>(storage_ptr);
        caster_ptr->timed_scan(time_stamp, _return);
        break;
    }
    case StorageType::type::LSMKeyValue: {

        auto caster_ptr = std::static_pointer_cast<LSMKeyValue<int64_t>>(storage_ptr);
        caster_ptr->timed_scan(time_stamp, _return);
        break;
    }
    case StorageType::type::HdfsKeyValue: {

        LOG(ERROR) << "HDFS ONLY SUPPORT STRING";
        break;
    }
    case StorageType::type::InMemoryImage: {

        LOG(ERROR) << "InMemoryImage ONLY SUPPORT STRING";
        break;
    }
    case StorageType::type::DenseMatrix: {

        auto caster_ptr = std::static_pointer_cast<DenseMatrix<int64_t>>(storage_ptr);
        caster_ptr->timed_scan(time_stamp, _return);
        break;
    }
    case StorageType::type::SparseMatrix: {

        LOG(ERROR) << "SPARSE MATRIX ONLY SUPPORT DOUBLE";
        break;
    }

    default: {
        break;
    }
    }
}

void TaskTrackerHandler::timed_scan_double(std::map<std::string, std::map<std::string, double> > & _return,
        const std::string& table_name,
        const int64_t shard_id,
        const std::string& cf_name,
        const int64_t time_stamp) {

    check_table(table_name, shard_id, cf_name);

    auto storage_ptr = StorageInfo::singleton()._cf_ptr[table_name][shard_id][cf_name];
    auto storage_type = StorageInfo::singleton()._cf_info[table_name][cf_name]._cf_property.storage_type;
 //   auto value_type = StorageInfo::singleton()._cf_info[table_name][cf_name]._cf_property.value_type;

    switch (storage_type) {

    case StorageType::type::InMemoryKeyValue:{

        auto caster_ptr = std::static_pointer_cast<InMemoryKeyValue<double>>(storage_ptr);
        caster_ptr->timed_scan(time_stamp, _return);
        break;
    }
    case StorageType::type::LSMKeyValue: {

        auto caster_ptr = std::static_pointer_cast<LSMKeyValue<double>>(storage_ptr);
        caster_ptr->timed_scan(time_stamp, _return);
        break;
    }
    case StorageType::type::HdfsKeyValue: {

        LOG(ERROR) << "HDFS ONLY SUPPORT STRING";
        break;
    }
    case StorageType::type::InMemoryImage: {

        LOG(ERROR) << "InMemoryImage ONLY SUPPORT STRING";
        break;
    }
    case StorageType::type::DenseMatrix: {

        auto caster_ptr = std::static_pointer_cast<DenseMatrix<double>>(storage_ptr);
        caster_ptr->timed_scan(time_stamp, _return);
        break;
    }
    case StorageType::type::SparseMatrix: {

        auto caster_ptr = std::static_pointer_cast<SparseMatrix<double>>(storage_ptr);
        caster_ptr->timed_scan(time_stamp, _return);
        break;
    }

    default: {
        break;
    }
    }
}

void TaskTrackerHandler::timed_scan_string(std::map<std::string, std::map<std::string, std::string> > & _return,
        const std::string& table_name,
        const int64_t shard_id,
        const std::string& cf_name,
        const int64_t time_stamp) {

    check_table(table_name, shard_id, cf_name);

    auto storage_ptr = StorageInfo::singleton()._cf_ptr[table_name][shard_id][cf_name];
    auto storage_type = StorageInfo::singleton()._cf_info[table_name][cf_name]._cf_property.storage_type;
//    auto value_type = StorageInfo::singleton()._cf_info[table_name][cf_name]._cf_property.value_type;

    switch (storage_type) {

    case StorageType::type::InMemoryKeyValue:{

        auto caster_ptr = std::static_pointer_cast<InMemoryKeyValue<std::string>>(storage_ptr);
        caster_ptr->timed_scan(time_stamp, _return);
        break;
    }
    case StorageType::type::LSMKeyValue: {

        auto caster_ptr = std::static_pointer_cast<LSMKeyValue<std::string>>(storage_ptr);
        caster_ptr->timed_scan(time_stamp, _return);
        break;
    }
    case StorageType::type::HdfsKeyValue: {

        auto caster_ptr = std::static_pointer_cast<HdfsKeyValue<std::string>>(storage_ptr);
        caster_ptr->timed_scan(time_stamp, _return);
        break;
    }
    case StorageType::type::InMemoryImage: {

        auto caster_ptr = std::static_pointer_cast<InMemoryImage<std::string>>(storage_ptr);
        caster_ptr->timed_scan(time_stamp, _return);
        break;
    }
    case StorageType::type::DenseMatrix: {

        LOG(ERROR) << "MATRIX ONLY SUPPORT NUM";
        break;
    }
    case StorageType::type::SparseMatrix: {

        LOG(ERROR) << "MATRIX ONLY SUPPORT NUM";
        break;
    }

    default: {
        break;
    }
    }
}


}
}


