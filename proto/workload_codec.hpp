//convert validated domain objects and protobuf messages
#pragma once

#include "client_service.pb.h"
#include "radahn/domain/workload.hpp"
#include "radahn/domain/execution_result.hpp"

namespace radahn::rpc_codec {

//domain -> protobuf, prepare workload for transmission
void encode_workload(
    const domain::WorkloadSpec& workload,
    rpc::v1::WorkloadSpec& output
);


[[nodiscard]]
//protobuf-> domain, validate received data and construrct domain obj
domain::WorkloadSpec decode_workload(
    const rpc::v1::WorkloadSpec& workload
);


void encode_execution_result(
    const domain::ExecutionResult& result,
    rpc::v1::ExecutionResult& output
);

[[nodiscard]]
domain::ExecutionResult decode_execution_result(
    const rpc::v1::ExecutionResult& input
);

} //namespace radahn::rpc_codec
