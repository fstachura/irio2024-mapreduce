from time import sleep
import logging
import os
from concurrent import futures
import grpc

from ..grpc.mapreduce_pb2 import LastJobStatusReply, Empty
from ..grpc.mapreduce_pb2_grpc import NodeAPIStub, UserAPIServicer, add_UserAPIServicer_to_server

class UserAPIServicerImpl(UserAPIServicer):
    def StartJob(self, request, context):
        return super().StartJob(request, context)

    def LastJobStatus(self, request, context):
        return LastJobStatusReply()

def query_node(addr):
    with grpc.insecure_channel(addr) as channel:
        stub = NodeAPIStub(channel)
        result = stub.NodeStatus(Empty())
        return result

def check_nodes(node_addr):
    while True:
        for addr in node_addr:
            try:
                result = query_node(addr)
                logging.info(f"node status of {addr}: {result.status}, errorMessage: {result.errorMessage}")
            except Exception:
                logging.exception("failed to query node " + str(addr))

        sleep(1)

def serve():
    with futures.ThreadPoolExecutor() as executor:
        node_addrs = os.environ.get("NODE_ADDRS", "")
        executor.submit(check_nodes, node_addrs.split(","))

        port = os.environ.get("HTTP_PORT", "[::]:50001")
        server = grpc.server(executor)
        add_UserAPIServicer_to_server(
            UserAPIServicerImpl(), server
        )
        server.add_insecure_port(port)
        server.start()
        server.wait_for_termination()

if __name__ == "__main__":
    logging.basicConfig()
    serve()

