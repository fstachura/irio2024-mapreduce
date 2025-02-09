import os
from queue import Empty
import sys
from time import sleep
from uuid import uuid4

import grpc
from google.protobuf.empty_pb2 import Empty
from mapreduce.proto import coordinator_pb2, coordinator_pb2_grpc
from mapreduce.worker.utils import get_file_handles_from_gstorage

TEST_DIRECTORY = os.path.dirname(__file__)

class Test:
    def __init__(self, test_name, input_filename_list, output_file):
        self.test_name = test_name
        self.input_filename_list = [os.path.join(TEST_DIRECTORY, filename) for filename in input_filename_list]
        self.output_file = os.path.join(TEST_DIRECTORY, output_file)

def upload_files(stub, filenames):
    input_location = f"irio_test:{uuid4}"
    for filename in filenames:
        input_path = f"{input_location}/{uuid4()}"
        [input_handle] = get_file_handles_from_gstorage([input_path])
        with open(filename, 'r') as r, input_handle.open('w') as w:
            w.write(r.read())

    return input_location

def run_test(stub, test: Test) -> bool:
    input_location = upload_files(stub, test.input_filename_list)
    output_location = f"irio_test:{uuid4()}"
    result = stub.StartJob(
        coordinator_pb2.StartJobRequest(
            inputLocation=input_location,
            outputLocation=output_location
        )
    )

    while not stub.LastJobStatus(Empty()).status.finished:
        sleep(5)

    [output_handle] = get_file_handles_from_gstorage([output_location])
    with output_handle.open('r') as calculated_output_file, open(test.output_file, 'r') as expected_output_file:
        calculated_output = calculated_output_file.read();
        expected_output = expected_output_file.read();
        if calculated_output != expected_output:
            print(test.test_name, "FAILED")
            print("EXPECTED:")
            print(expected_output)
            print("GOT:")
            print(calculated_output)
            return False
        else:
            print(test.test_name, "PASSED")
            return True

def run_tests():
    if len(sys.argv) < 2:
        print("usage:", sys.argv[0], "host:port")
        return
    host_port = sys.argv[1]

    tests = [Test("single_word", ["single_word_input"], "single_word_output")]
    failed_tests = []
    for test in tests:
        with grpc.insecure_channel(host_port) as channel:
            stub = coordinator_pb2_grpc.CoordinatorServiceStub(channel)
            if not run_test(stub, test):
                failed_tests.append(test)

    if len(failed_tests) == 0:
        print("ALL TESTS PASSED")
    else:
        print("FAILED TESTS:", tests)

if __name__ == "__main__":
    run_tests()