from time import sleep
import logging
import os
from concurrent import futures
import grpc
import dns.resolver
from google.protobuf.empty_pb2 import Empty

from ..proto.coordinator_pb2 import LastJobStatusReply
from ..proto.coordinator_pb2_grpc import CoordinatorServiceServicer, add_CoordinatorServiceServicer_to_server
from ..proto.worker_pb2_grpc import WorkerServiceStub

class CoordinatorServiceServicerImpl(CoordinatorServiceServicer):
    def StartJob(self, request, context):
        return super().StartJob(request, context)

    def LastJobStatus(self, request, context):
        return LastJobStatusReply()

def query_node(addr, node_port):
    with grpc.insecure_channel(addr + ":" + node_port) as channel:
        stub = WorkerServiceStub(channel)
        result = stub.WorkerStatus(Empty())
        return result

def check_nodes(nodes_addr, node_port):
    while True:
        nodes = dns.resolver.resolve(nodes_addr, 'A')
        for addr in nodes:
            try:
                result = query_node(addr.address, node_port)
                logging.info(f"node status of {addr}: {result.status}, errorMessage: {result.errorMessage}, workerUuid: {result.workerUuid}")
            except Exception:
                logging.exception("failed to query node " + str(addr))

        sleep(1)

def serve():
    with futures.ThreadPoolExecutor() as executor:
        nodes_addr = os.environ.get("NODES_ADDR", "")
        node_port = os.environ.get("NODE_PORT", "50051")
        executor.submit(check_nodes, nodes_addr, node_port)

        port = os.environ.get("HTTP_PORT", "[::]:50001")
        server = grpc.server(executor)
        add_CoordinatorServiceServicer_to_server(
            CoordinatorServiceServicerImpl(), server
        )
        server.add_insecure_port(port)
        server.start()
        server.wait_for_termination()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    serve()
