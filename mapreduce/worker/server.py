from concurrent import futures
import logging
import grpc
from ..grpc import mapreduce_pb2
from ..grpc import mapreduce_pb2_grpc
from ..grpc.mapreduce_pb2_grpc import NodeAPIServicer

class NodeAPIServicerImpl(NodeAPIServicer):
    def StartStep(self, request, context):
        print("StartStep request:\n" + str(request))
        return mapreduce_pb2.StartStepReply(ok=True)
    
    def NodeStatus(self, request, context):
        print("NodeStatus request: " + str(request))
        status = mapreduce_pb2.NodeStatusReply.NodeStatusEnum.Ok
        return mapreduce_pb2.NodeStatusReply(status=status)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    mapreduce_pb2_grpc.add_NodeAPIServicer_to_server(
        NodeAPIServicerImpl(), server
    )
    server.add_insecure_port("[::]:50051")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    logging.basicConfig()
    serve()