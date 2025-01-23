from time import sleep
import logging
import grpc
import dns.resolver
from google.protobuf.empty_pb2 import Empty
from sqlalchemy.orm import Session

from ..proto.worker_pb2_grpc import WorkerServiceStub
from ..proto.worker_pb2 import StartStepRequest, WorkerStatusReply

from .algorithm import execute_next_step
from .utils import generate_tmp_location, get_blob, get_unfinished_job, get_unfinished_job_parts

logger = logging.getLogger(__name__)

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
            logger.info(f"node status of {addr}: {result.status}, uuid: {result.workerUuid}, "
                        f"errorMessage: {result.errorMessage}")
        except Exception:
            logger.exception("failed to query node " + str(addr))

    return stats

def restart_part(tr, part):
    part.executor_node_uuid = None
    part.output_location = None
    tr.commit()

def submit_job_to_worker(tr, addr, node_port, job, part):
    try:
        logger.info(f"attempting to submit job to worker {addr} {part.step} {part.input_location}")
        with grpc.insecure_channel(addr + ":" + node_port) as channel:
            stub = WorkerServiceStub(channel)
            output_location = generate_tmp_location(job.input_location, part.step)
            request = StartStepRequest(
                    stepId=part.step,
                    inputLocation=part.input_location,
                    outputLocation=output_location)
            result = stub.StartStep(request)
            if result.ok:
                # NOTE if coordinator crashes after submitting job, but before saving job to the database,
                # that progress will effectively be lost. the part will get resubmitted later.
                # coordinator does not have a way to "learn" which parts are being done from workers while recovering
                part.executor_node_uuid = result.workerUuid
                part.output_location = output_location
                tr.commit()
                logger.info(f"job submitted to worker {addr} {part.executor_node_uuid}"
                             f"{part.step} {part.input_location} {part.output_location}")
            else:
                logger.error(f"worker returned ok=False after attempting to submit a job {addr}")
    except Exception:
        logger.exception(f"failed to submit job to worker {addr}")

class UpdateContext:
    def __init__(self, db, storage_client, get_nodes):
        self.db = db
        self.storage_client = storage_client
        self.get_nodes = get_nodes

def update(tr, ctx: UpdateContext):
    # if all parts finished, execute next step - collect all parts, pass to reducer etc
    # NOTE: it is assumed, that while multiple threads can create job parts, only a single thread (at the same time) manipulates job statuses.
    nodes = get_node_stats(ctx.get_nodes())
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
                    logger.info(f"worker finished part {part.step} {part.output_location} {part.executor_node_uuid}")
                else:
                    # worker ready but file not found, restart
                    restart_part(tr, part)
                    logger.info(f"file not found despite worker being ready {part.executor_node_uuid}"
                                 f"{part.step} {part.output_location}")

            elif node[1].status == WorkerStatusReply.WorkerStatusEnum.Failure:
                # worker failed
                logger.error(f"worker failed with {node.errorMessage}")
                restart_part(tr, part)
            elif node[1].status == WorkerStatusReply.WorkerStatusEnum.Working:
                pass
            else:
                logger.error(f"unknown status {node.status}")

    # assign unfinished jobs to workers
    for part in parts:
        if str(part.executor_node_uuid) not in nodes_by_uuid:
            if part.executor_node_uuid is None:
                # job was never assigned
                if len(free_nodes) > 0:
                    node = free_nodes.pop()
                    submit_job_to_worker(tr, node[0][0], node[0][1], job, part)
            else:
                # node went missing
                # TODO maybe assume that the node went missing after failing to contact it three times in a row?
                part.executor_node_uuid = None
                tr.commit()
                if len(free_nodes) > 0:
                    node = free_nodes.pop()
                    submit_job_to_worker(tr, node[0][0], node[0][1], job, part)

    tr.commit()

    if len(get_unfinished_job_parts(tr)) == 0:
        logger.info(f"step finished {job.current_step}")
        execute_next_step(tr, ctx.storage_client, job)

def start_update_loop(ctx: UpdateContext):
    while True:
        try:
            with Session(ctx.db) as tr:
                update(tr, ctx)
        except:
            logger.exception("failed to update")
        sleep(1)

