import logging
import os
from concurrent import futures
import grpc
from google.cloud import storage

from ..proto.coordinator_pb2_grpc import add_CoordinatorServiceServicer_to_server

from .coordinator_service import CoordinatorServiceServicerImpl
from .database import connect_to_db
from .update_loop import start_update_loop, UpdateContext, get_nodes_from_dns
from .utils import AtomicInt

logger = logging.getLogger(__name__)

def serve():
    with futures.ThreadPoolExecutor() as executor:
        nodes_addr = os.environ.get("NODES_ADDR", "")
        node_port = os.environ.get("NODE_PORT", "50051")
        default_bucket = os.environ["DEFAULT_BUCKET"]

        port = os.environ.get("HTTP_PORT", "[::]:50001")
        db = connect_to_db()
        storage_client = storage.Client()
        get_nodes = lambda: get_nodes_from_dns(nodes_addr, node_port)
        #get_nodes = lambda: [("localhost", "50051"), ("localhost", "50052"), ("localhost", "50053")]
        expected_parts = AtomicInt(len(get_nodes()))

        executor.submit(start_update_loop, UpdateContext(db, storage_client, get_nodes, expected_parts))

        server = grpc.server(executor)
        add_CoordinatorServiceServicer_to_server(
            CoordinatorServiceServicerImpl(db, default_bucket, expected_parts), server
        )
        server.add_insecure_port(port)
        server.start()
        server.wait_for_termination()

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s:%(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    serve()

