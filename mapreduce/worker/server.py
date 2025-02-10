from concurrent import futures
import logging
import os
import grpc
from grpc_health.v1.health import HealthServicer
from grpc_health.v1.health_pb2 import HealthCheckResponse
from grpc_health.v1.health_pb2_grpc import add_HealthServicer_to_server

from .worker_service import WorkerServiceServicerImpl
from ..proto import worker_pb2_grpc

def serve():
    # Worker node processes one request at a time.
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    worker_pb2_grpc.add_WorkerServiceServicer_to_server(
        WorkerServiceServicerImpl(), server
    )

    health_servicer = HealthServicer()
    health_servicer.set("WorkerService", HealthCheckResponse.SERVING)
    add_HealthServicer_to_server(
        health_servicer, server
    )

    port = os.environ.get("HTTP_PORT", "[::]:50051")
    server.add_insecure_port(port)
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    serve()
