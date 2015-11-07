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

#ifndef NTU_CAP_UNICAP_JOB_TRACKER_JOB_TRACKER_SERVER_H_
#define NTU_CAP_UNICAP_JOB_TRACKER_JOB_TRACKER_SERVER_H_

#include <stdint.h>
#include <thread>
#include <iostream>

#include <thrift/concurrency/ThreadManager.h>
#include <thrift/concurrency/PlatformThreadFactory.h>
#include <thrift/concurrency/PosixThreadFactory.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/server/TThreadPoolServer.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>
#include <thrift/TToString.h>

#include "./job_tracker_handler.h"
#include "../gen/JobTracker.h"
#include "../gen/TaskTracker.h"
#include "../common/unicap_client.h"
#include "../common/table.h"
#include "../common/column_family.h"

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using namespace ::apache::thrift::concurrency;

namespace ntu {
namespace cap {

class JobTrackerServer {
public:
    JobTrackerServer();

    static JobTrackerServer& singleton() {
        static JobTrackerServer server;
        return server;
    }

    int64_t set_port(int64_t port);

    int64_t set_thread_num(int64_t thread_num);

    int64_t create_task_tracker_client();

    int64_t check_client_task_tracker();

    std::thread start();

    static int64_t serve(boost::shared_ptr<TThreadPoolServer> &server) {
        server->serve();
        return 1;
    }

private:
    int64_t _port;
    int64_t _thread_num;

    boost::shared_ptr<JobTrackerHandler>  _handler;
    boost::shared_ptr<TProcessor>         _processor;
    boost::shared_ptr<TServerTransport>   _serverTransport;
    boost::shared_ptr<TTransportFactory>  _transportFactory;
    boost::shared_ptr<TProtocolFactory>   _protocolFactory;
    boost::shared_ptr<ThreadManager>      _threadManager;

    boost::shared_ptr<PosixThreadFactory> _threadFactory;
    boost::shared_ptr<TThreadPoolServer>  _server;
};

}//end namspace ntu
}//end namespace cap

#endif /* UNICAP_JOB_TRACKER_JOB_TRACKER_SERVER_H_ */
