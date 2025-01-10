from concurrent import futures
import logging
import os
import grpc
import uuid
from ..grpc import mapreduce_pb2
from ..grpc import mapreduce_pb2_grpc
from ..grpc.mapreduce_pb2_grpc import NodeAPIServicer

class NodeAPIServicerImpl(NodeAPIServicer):
    def __init__(self):
        super().__init__()
        self.workerUuid = str(uuid.uuid4())

    def StartStep(self, request, context):
        logging.info("StartStep request:\n" + str(request))
        return mapreduce_pb2.StartStepReply(ok=True, workerUuid=self.workerUuid)
    
    def NodeStatus(self, request, context):
        logging.info("NodeStatus request: " + str(request))
        status = mapreduce_pb2.NodeStatusReply.NodeStatusEnum.Ok
        return mapreduce_pb2.NodeStatusReply(status=status, workerUuid=self.workerUuid)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
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