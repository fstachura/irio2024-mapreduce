from time import sleep
import logging
import os
from concurrent import futures
import grpc
import dns.resolver
from google.protobuf.empty_pb2 import Empty
from google.cloud import storage
from sqlalchemy.orm import Session

from ..proto.coordinator_pb2 import LastJobStatusReply, StartJobReply
from ..proto.coordinator_pb2_grpc import CoordinatorServiceServicer, add_CoordinatorServiceServicer_to_server
from ..proto.worker_pb2_grpc import WorkerServiceStub
from .database import JobPart, connect_to_db, Job

WORDCOUNT_STEPS = [
    {
        "name": "map",
        "shared": True,
    },
    {
        "name": "shuffle",
        "shared": False,
        "function": None,
    },
    {
        "name": "reduce",
        "shared": True,
    },
    {
        "name": "collect",
        "shared": False,
        "function": None,
    }
]

class CoordinatorServiceServicerImpl(CoordinatorServiceServicer):
    def __init__(self, db):
        self.db = db

    def StartJob(self, request, context):
        storage_client = storage.Client()
        bucket_name, directory_name = request.inputLocation.split(':')
        bucket = storage_client.bucket(bucket_name)

        with Session(self.db) as tr:
            output_location = request.inputLocation.rstrip("/") + "/map/"
            job = Job(input_location=request.inputLocation, current_step=WORDCOUNT_STEPS[0]["name"])
            tr.add(job)
            tr.flush()
            tr.refresh(job)

            i = 0
            for f in bucket.list_blobs(prefix=directory_name):
                if f.name.rstrip("/") != directory_name:
                    part = JobPart(input_location=f.name,
                                   output_location=output_location + str(i),
                                   finished=False,
                                   job_id=job.id,
                                   step=WORDCOUNT_STEPS[0]["name"])
                    tr.add(part)
                    i += 1

            tr.commit()

            return StartJobReply(jobUuid=str(job.job_uuid))

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
        db = connect_to_db()

        server = grpc.server(executor)
        add_CoordinatorServiceServicer_to_server(
            CoordinatorServiceServicerImpl(db), server
        )
        server.add_insecure_port(port)
        server.start()
        server.wait_for_termination()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    serve()
