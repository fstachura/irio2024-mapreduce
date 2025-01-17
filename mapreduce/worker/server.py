from concurrent import futures
import logging
import os
import grpc

from .worker_service import NodeAPIServicerImpl
from ..grpc import mapreduce_pb2_grpc

def serve():
    # Worker node processes one request at a time.
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    mapreduce_pb2_grpc.add_NodeAPIServicer_to_server(
        NodeAPIServicerImpl(), server
    )
    port = os.environ.get("HTTP_PORT", "[::]:50051")
    server.add_insecure_port(port)
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    serve()