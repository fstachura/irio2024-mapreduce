import logging
import grpc
from ..grpc import mapreduce_pb2
from ..grpc import mapreduce_pb2_grpc

def client():
    with grpc.insecure_channel("localhost:50051") as channel:
        stub = mapreduce_pb2_grpc.NodeAPIStub(channel)

        startStepRequest = mapreduce_pb2.StartStepRequest(
            inputLocation="inputLocationPlaceholder",
            outputLocation="outputLocationPlaceholder",
            stepId="stepIdPlaceholder"
        )
        startStepReply = stub.StartStep(startStepRequest)
        print("StartStep(..): " + str(startStepReply.ok))

        nodeStatusReply = stub.NodeStatus(mapreduce_pb2.Empty())
        print("NodeStatus(): " + str(nodeStatusReply.status))

if __name__ == "__main__":
    logging.basicConfig()
    client()