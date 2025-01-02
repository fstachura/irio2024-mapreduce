mapreduce/grpc/mapreduce_pb2.py: mapreduce/grpc/mapreduce.proto
	python3 -m grpc_tools.protoc -I mapreduce/grpc/ --python_out=mapreduce/grpc/ --grpc_python_out=mapreduce/grpc/ mapreduce/grpc/mapreduce.proto

clean:
	rm mapreduce/grpc/*.py

