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
#ifndef NTU_CAP_UNICAP_TASK_TRACKER_TO_JOB_TRACKER_CLIENT_H_
#define NTU_CAP_UNICAP_TASK_TRACKER_TO_JOB_TRACKER_CLIENT_H_

#include <iostream>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TTransportUtils.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include "../gen/JobTracker.h"
#include "../gen/TaskTracker.h"

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

namespace ntu {
namespace cap {

template <class T>
class UnicapClient {

public:
    UnicapClient(const std::string& host_name, const int64_t port) {
        _host_name = host_name;
        _port      = port;
        _socket    = boost::shared_ptr<TSocket>(new TSocket(_host_name, _port));
        _transport = boost::shared_ptr<TTransport>(new TBufferedTransport(_socket));
        _protocol  = boost::shared_ptr<TProtocol>(new TBinaryProtocol(_transport));
        _client    = boost::shared_ptr<T>(new T(_protocol));
    }

    int64_t open_transport() {
        _transport->open();
        return 1;
    }

    int64_t close_transport() {
        _transport->close();
        return 1;
    }

    boost::shared_ptr<T>& method() {
        return _client;
    }

private:
    std::string _host_name;
    int64_t     _port;
    boost::shared_ptr<TSocket>    _socket;
    boost::shared_ptr<TTransport> _transport;
    boost::shared_ptr<TProtocol>  _protocol;
    boost::shared_ptr<T>          _client;
};

}
}

#endif /* UNICAP_TASK_TRACKER_TO_JOB_TRACKER_CLIENT_H_ */
