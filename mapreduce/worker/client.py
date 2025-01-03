import logging
import sys
import grpc
from ..grpc import mapreduce_pb2
from ..grpc import mapreduce_pb2_grpc

def client():
    if len(sys.argv) != 2:
        print("usage: ", sys.argv[0], "host:port")
        return

    with grpc.insecure_channel(sys.argv[1]) as channel:
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