import logging
import sys
import grpc
from ..proto import mapreduce_pb2
from ..proto import mapreduce_pb2_grpc

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
        print("StartStep(..):\n" + str(startStepReply))

        nodeStatusReply = stub.NodeStatus(mapreduce_pb2.Empty())
        print("NodeStatus():\n" + str(nodeStatusReply))

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    client()
