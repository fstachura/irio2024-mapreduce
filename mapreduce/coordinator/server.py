from time import sleep
import logging
import os
from concurrent import futures
import grpc
import dns.resolver

from ..proto.mapreduce_pb2 import LastJobStatusReply, Empty
from ..proto.mapreduce_pb2_grpc import NodeAPIStub, UserAPIServicer, add_UserAPIServicer_to_server

class UserAPIServicerImpl(UserAPIServicer):
    def StartJob(self, request, context):
        return super().StartJob(request, context)

    def LastJobStatus(self, request, context):
        return LastJobStatusReply()

def query_node(addr, node_port):
    with grpc.insecure_channel(addr + ":" + node_port) as channel:
        stub = NodeAPIStub(channel)
        result = stub.NodeStatus(Empty())
        return result

def check_nodes(nodes_addr, node_port):
    while True:
        nodes = dns.resolver.resolve(nodes_addr, 'A')
        for addr in nodes:
            try:
                result = query_node(addr.address, node_port)
                logging.info(f"node status of {addr}: {result.status}, errorMessage: {result.errorMessage}")
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
        add_UserAPIServicer_to_server(
            UserAPIServicerImpl(), server
        )
        server.add_insecure_port(port)
        server.start()
        server.wait_for_termination()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    serve()

