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
#include "./task_tracker_server.h"
namespace ntu {
namespace cap {

TaskTrackerServer::TaskTrackerServer() {
    _port        = 9010;
    _thread_num  = 0;

    MPI_Comm_rank(MPI_COMM_WORLD, &NodeInfo::singleton()._node_id);
    MPI_Comm_size(MPI_COMM_WORLD, &NodeInfo::singleton()._node_num);
    MPI_Get_processor_name(NodeInfo::singleton()._processor_name,
                           &NodeInfo::singleton()._name_length);
    NodeInfo::singleton()._host_name =
        std::string(NodeInfo::singleton()._processor_name,
                    NodeInfo::singleton()._name_length);

    _thread_num = NodeInfo::singleton()._node_num;
    NodeInfo::singleton()._client_job_tracker =
        boost::shared_ptr<UnicapClient<JobTrackerClient>>
        (new UnicapClient<JobTrackerClient>(NodeInfo::singleton()._master_host_name,
                                            NodeInfo::singleton()._master_port));
}

int64_t TaskTrackerServer::set_thread_num(int64_t thread_num) {
    _thread_num = thread_num;
    return 1;
}

int64_t TaskTrackerServer::regeister() {
    NodeInfo::singleton()._client_job_tracker->open_transport();
    _port = NodeInfo::singleton()._client_job_tracker ->
            method() ->
            register_task_tracker(
                    NodeInfo::singleton()._node_id,
                    NodeInfo::singleton()._processor_name,
                    NodeInfo::singleton()._storage_weight);
    NodeInfo::singleton()._port = _port;

    NodeInfo::singleton()._client_job_tracker ->
            close_transport();
    return 1;
}

int64_t TaskTrackerServer::fetch_node_info() {
    NodeInfo::singleton()._client_job_tracker ->
            open_transport();
    NodeInfo::singleton()._client_job_tracker ->
            method() ->
            get_all_task_tracker_info(NodeInfo::singleton()._task_tracker_info);

    NodeInfo::singleton()._client_job_tracker ->
            close_transport();
    return 1;
}

int64_t TaskTrackerServer::create_task_tracker_client() {
    for (auto& kvp : NodeInfo::singleton()._task_tracker_info) {
        NodeInfo::singleton()._client_task_tracker[kvp.first] =
            boost::shared_ptr<UnicapClient<TaskTrackerClient>>
            (new UnicapClient<TaskTrackerClient>(kvp.second.host_name,
                                                kvp.second.port));
    }
    return 1;
}

int64_t TaskTrackerServer::check_client_task_tracker() {
    DLOG(INFO) << NodeInfo::singleton()._node_id
               << " ("
               << NodeInfo::singleton()._host_name
               << " : "
               << NodeInfo::singleton()._port
               << " -> "
               << "CHECK NETWORK CONNECTION";
    for (auto i : NodeInfo::singleton()._client_task_tracker) {
        std::string re;
        i.second->open_transport();
        i.second->method()->ping(re);
        CHECK_EQ(re, "Pong");
        i.second->close_transport();
    }
    return 1;
}

std::thread TaskTrackerServer::start() {
    _handler          = boost::shared_ptr<TaskTrackerHandler>(new TaskTrackerHandler());
    _processor        = boost::shared_ptr<TProcessor>(new TaskTrackerProcessor(_handler));
    _serverTransport  = boost::shared_ptr<TServerTransport>(new TServerSocket(_port));
    _transportFactory = boost::shared_ptr<TTransportFactory>(new TBufferedTransportFactory());
    _protocolFactory  = boost::shared_ptr<TProtocolFactory>(new TBinaryProtocolFactory());

    _threadManager    = boost::shared_ptr<ThreadManager>(ThreadManager::newSimpleThreadManager(_thread_num));
    _threadFactory    = boost::shared_ptr<PosixThreadFactory>(new PosixThreadFactory());

    _threadManager->threadFactory(_threadFactory);
    _threadManager->start();
    _server =  boost::shared_ptr<TThreadPoolServer>(new TThreadPoolServer(_processor,
               _serverTransport,
               _transportFactory,
               _protocolFactory,
               _threadManager));
    std::thread thread_server(serve, std::ref(_server));

    return thread_server;
}

}
}
