from concurrent import futures
import logging
import os
import grpc
from google.protobuf.empty_pb2 import Empty

from ..proto import worker_pb2
from ..proto import worker_pb2_grpc
from ..proto.worker_pb2_grpc import WorkerServiceServicer

class WorkerServiceServicerImpl(WorkerServiceServicer):
    def StartStep(self, request, context):
        logging.info("StartStep request:\n" + str(request))
        return worker_pb2.StartStepReply(ok=True)
    
    def WorkerStatus(self, request, context):
        logging.info("WorkerStatus request: " + str(request))
        status = worker_pb2.WorkerStatusReply.WorkerStatusEnum.Ok
        return worker_pb2.WorkerStatusReply(status=status)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    worker_pb2_grpc.add_WorkerServiceServicer_to_server(
        WorkerServiceServicerImpl(), server
    )
    port = os.environ.get("HTTP_PORT", "[::]:50051")
    server.add_insecure_port(port)
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    logging.basicConfig()
    serve()
