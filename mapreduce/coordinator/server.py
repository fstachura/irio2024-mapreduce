from time import sleep
import logging
import os
import uuid
from pathlib import Path
from concurrent import futures
import grpc
import dns.resolver
from google.protobuf.empty_pb2 import Empty
from google.cloud import storage
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..proto.coordinator_pb2 import LastJobStatusReply, StartJobReply
from ..proto.coordinator_pb2_grpc import CoordinatorServiceServicer, add_CoordinatorServiceServicer_to_server
from ..proto.worker_pb2 import StartStepRequest, WorkerStatusReply
from ..proto.worker_pb2_grpc import WorkerServiceStub
from .database import JobPart, connect_to_db, Job

def start_map(tr, storage_client, job):
    bucket_name, directory_name = job.input_location.split(':')
    bucket = storage_client.bucket(bucket_name)
    if directory_name[-1] != "/":
        directory_name += "/"

    for f in bucket.list_blobs(prefix=directory_name):
        if f.name.rstrip("/") != directory_name.rstrip("/"):
            logging.info(f"processing file in start_map {f}")
            part = JobPart(input_location=f"{bucket_name}:{f.name}",
                           finished=False,
                           job_id=job.id,
                           step="map")
            tr.add(part)

    tr.commit()

def execute_shuffle(tr, storage_client, job):
    # download all data (only from job parts)
    # sort
    # split into n nodes
    # upload
    # create reduce job_part for each output file
    # all in a transaction - if coordinator crashes, neither the transition nor job parts get saved
    # what if coordinator fails during that? execute shuffle will get re-executed from the loop (assuming i don't mess up transactions)
    pass

def execute_collect(tr, storage_client, job):
    # concat all reduce output files into a new file
    # what if coordinator fails during that? will get re-executed from the loop
    pass

WORDCOUNT_STEPS = {
    "map": {
        "next": "shuffle",
        "callback": start_map,
    },
    "shuffle": {
        "next": "reduce",
        # starts reduce job parts
        "callback": execute_shuffle,
        "next": "collect"
    },
    "collect": {
        "callback": execute_collect,
    }
}

class CoordinatorServiceServicerImpl(CoordinatorServiceServicer):
    def __init__(self, db):
        self.db = db

    def StartJob(self, request, context):
        with Session(self.db) as tr:
            storage_client = storage.Client()

            job = Job(input_location=request.inputLocation, current_step="map")
            tr.add(job)
            tr.flush()
            tr.refresh(job)

            WORDCOUNT_STEPS["map"]["callback"](tr, storage_client, job)
            return StartJobReply(jobUuid=str(job.job_uuid))

    def LastJobStatus(self, request, context):
        status = None
        with Session(self.db) as tr:
            job = get_unfinished_job(tr)
            if job is not None:
                job_finished = job.finished is None or job.finished == True
                status = LastJobStatusReply.JobStatus(jobUuid=str(job.job_uuid), finished=job_finished)
        return LastJobStatusReply(status=status)

def query_node(addr, node_port):
    with grpc.insecure_channel(addr + ":" + node_port) as channel:
        stub = WorkerServiceStub(channel)
        result = stub.WorkerStatus(Empty())
        return result

def get_nodes_from_dns(nodes_addr, node_port):
    return [(addr.address, node_port) for addr in dns.resolver.resolve(nodes_addr, 'A')]

def get_node_stats(nodes):
    stats = []
    for (addr, node_port) in nodes:
        try:
            result = query_node(addr, node_port)
            stats.append(((addr, node_port), result))
            logging.info(f"node status of {addr}: {result.status}, uuid: {result.workerUuid}, "
                        f"errorMessage: {result.errorMessage}")
        except Exception:
            logging.exception("failed to query node " + str(addr))

    return stats

def get_unfinished_job_parts(tr):
    stmt = select(JobPart).where(JobPart.finished == False)
    return tr.execute(stmt).scalars().all()

def get_unfinished_job(tr):
    stmt = select(Job).where(Job.finished == False)
    return tr.execute(stmt).scalars().first()

def get_blob(location):
    storage_client = storage.Client()
    bucket_name, filename = location.split(':')
    bucket = storage_client.bucket(bucket_name)
    return bucket.get_blob(filename)

def restart_part(tr, part):
    part.executor_node_uuid = None
    part.output_location = None
    tr.commit()

def generate_output_location(input_location, step):
    return str(Path(input_location).parent).rstrip("/") + f"_output/{step}/" + str(uuid.uuid4())

def submit_job_to_worker(tr, addr, node_port, part):
    try:
        logging.info(f"attempting to submit job to worker {addr} {part.step} {part.input_location}")
        with grpc.insecure_channel(addr + ":" + node_port) as channel:
            stub = WorkerServiceStub(channel)
            output_location = generate_output_location(part.input_location, part.step)
            request = StartStepRequest(
                    stepId=part.step, 
                    inputLocation=part.input_location,
                    outputLocation=output_location)
            result = stub.StartStep(request)
            if result.ok:
                # NOTE if coordinator crashes after submitting job, but before saving job to the database, 
                # that progress will effectively be lost. the part will get resubmitted later.
                # coordinator does not have a way to "learn" which parts are being done from workers, after booting up
                print(result)
                part.executor_node_uuid = result.workerUuid
                part.output_location = output_location
                tr.commit()
                logging.info(f"job submitted to worker {addr} {part.executor_node_uuid}"
                             f"{part.step} {part.input_location} {part.output_location}")
            else:
                logging.error(f"worker returned ok=False after attempting to submit a job {addr}")
    except Exception:
        logging.exception(f"failed to submit job to worker {addr}")

def update(tr, get_nodes):
    # if all parts finished, execute next step - collect all parts, pass to reducer etc
    # NOTE: it is assumed, that while multiple threads can create job parts, only a single thread (at the same time) manipulates job statues.
    nodes = get_node_stats(get_nodes())
    nodes_by_uuid = {stats[1].workerUuid:stats for stats in nodes}
    free_nodes = [n for n in nodes if n[1].status == WorkerStatusReply.WorkerStatusEnum.Ok]
    job = get_unfinished_job(tr)
    if job is None:
        return

    parts = get_unfinished_job_parts(tr)

    # check if any jobs were finished
    for part in parts:
        if str(part.executor_node_uuid) in nodes_by_uuid:
            # job is being executed by a node
            node = nodes_by_uuid[str(part.executor_node_uuid)]
            if node[1].status == WorkerStatusReply.WorkerStatusEnum.Ok:
                if get_blob(part.output_location) is not None:
                    # job is finished
                    part.finished = True
                    tr.commit()
                    logging.info(f"worker finished part {part.step} {part.output_location} {part.executor_node_uuid}")
                else:
                    # worker ready but file not found, restart
                    restart_part(tr, part)
                    logging.info(f"file not found despite worker being ready {part.executor_node_uuid}" 
                                 f"{part.step} {part.output_location}")

            elif node[1].status == WorkerStatusReply.WorkerStatusEnum.Failure:
                # worker failed
                logging.error(f"worker failed with {node.errorMessage}")
                restart_part(tr, part)
            elif node[1].status == WorkerStatusReply.WorkerStatusEnum.Working:
                pass
            else:
                logging.error(f"unknown status {node.status}")

    # assign unfinished jobs to workers
    for part in parts:
        if str(part.executor_node_uuid) not in nodes_by_uuid:
            if part.executor_node_uuid is None:
                # job was never assigned
                if len(free_nodes) > 0:
                    node = free_nodes.pop()
                    submit_job_to_worker(tr, node[0][0], node[0][1], part)
            else:
                # node went missing
                # TODO maybe assume that the node went missing after failing to contact it three times in a row?
                part.executor_node_uuid = None
                tr.commit()
                if len(free_nodes) > 0:
                    node = free_nodes.pop()
                    submit_job_to_worker(tr, node[0][0], node[0][1], part)

    tr.commit()

    if len(get_unfinished_job_parts(tr)) == 0:
        logging.info(f"step finished {job.current_step}")
        next_step_name = WORDCOUNT_STEPS[job.current_step]["next"]
        WORDCOUNT_STEPS[next_step_name]["callable"]()

def start_update_loop(db, get_nodes):
    while True:
        try:
            with Session(db) as tr:
                update(tr, get_nodes)
        except:
            logging.exception("failed to update")
        sleep(1)

def serve():
    with futures.ThreadPoolExecutor() as executor:
        nodes_addr = os.environ.get("NODES_ADDR", "")
        node_port = os.environ.get("NODE_PORT", "50051")

        port = os.environ.get("HTTP_PORT", "[::]:50001")
        db = connect_to_db()
        #get_nodes = lambda: get_nodes_from_dns(nodes_addr, node_port)
        get_nodes = lambda: [("localhost", "50051"), ("localhost", "50052"), ("localhost", "50053")]
        executor.submit(start_update_loop, db, get_nodes)

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
