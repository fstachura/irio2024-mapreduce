import logging
from sqlalchemy import select

from .database import JobPart
from .utils import split_on_same_elements, generate_tmp_location

logger = logging.getLogger(__name__)

WORDCOUNT_MAP = "map"
WORDCOUNT_SHUFFLE = "shuffle"
WORDCOUNT_REDUCE = "reduce"
WORDCOUNT_COLLECT = "collect"

def start_map(tr, storage_client, job):
    bucket_name, directory_name = job.input_location.split(':')
    bucket = storage_client.bucket(bucket_name)
    if directory_name[-1] != "/":
        directory_name += "/"

    for f in bucket.list_blobs(prefix=directory_name):
        if f.name.rstrip("/") != directory_name.rstrip("/"):
            logger.info(f"processing file in start_map {f}")
            part = JobPart(input_location=f"{bucket_name}:{f.name}",
                           finished=False,
                           job_id=job.id,
                           step=WORDCOUNT_MAP)
            tr.add(part)

    tr.commit()

def execute_shuffle(tr, storage_client, job):
    stmt = select(JobPart).where(JobPart.step == WORDCOUNT_MAP, JobPart.job_id == job.id)
    parts = tr.execute(stmt).scalars().all()
    result = []

    # NOTE doing an in memory sort and split (which technically defeats the whole purpose) for now
    for part in parts:
        assert part.finished

        bucket_name, directory_name = part.output_location.split(':')
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.get_blob(directory_name)
        with blob.open('r') as f:
            result.extend(line.split(',') for line in f.read().split('\n') if len(line.strip()) != 0)

    result.sort(key=lambda x: x[0])
    reduce_parts = split_on_same_elements(result, len(parts), lambda x, y: x[0] == y[0])
    reduce_parts = [p for p in reduce_parts if len(p) != 0]

    job_bucket_name, input_directory = job.input_location.split(":")
    job_bucket = storage_client.bucket(job_bucket_name)

    for reduce_part in reduce_parts:
        input_location = generate_tmp_location(input_directory , WORDCOUNT_REDUCE + "_input")
        with job_bucket.blob(input_location).open('w') as f:
            for part in reduce_part:
                f.write(part[0] + "," + part[1] + "\n")

        part = JobPart(input_location=f"{job_bucket_name}:{input_location}",
                       finished=False,
                       job_id=job.id,
                       step=WORDCOUNT_REDUCE)
        tr.add(part)

    job.current_step = WORDCOUNT_REDUCE
    tr.commit()
    # in a transaction - if coordinator crashes, neither the transition nor job parts get saved

def execute_collect(tr, storage_client, job):
    stmt = select(JobPart).where(JobPart.step == WORDCOUNT_REDUCE, JobPart.job_id == job.id)
    parts = tr.execute(stmt).scalars().all()

    output_bucket_name, output_file = job.output_location.split(':')
    output_bucket = storage_client.bucket(output_bucket_name)

    with output_bucket.blob(output_file).open('w') as out:
        for part in parts:
            assert part.finished

            bucket_name, directory_name = part.output_location.split(':')
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.get_blob(directory_name)
            with blob.open('r') as f:
                out.write(f.read())

    job.finished = None
    tr.commit()


WORDCOUNT_STEPS = {
    WORDCOUNT_MAP: {
        "next": WORDCOUNT_SHUFFLE,
        "callback": start_map,
    },
    WORDCOUNT_SHUFFLE: {
        # starts reduce job parts
        "callback": execute_shuffle,
    },
    WORDCOUNT_REDUCE: {
        "next": WORDCOUNT_COLLECT,
    },
    WORDCOUNT_COLLECT: {
        "callback": execute_collect,
    }
}

def init_algorithm(tr, storage_client, job):
    WORDCOUNT_STEPS[WORDCOUNT_MAP]["callback"](tr, storage_client, job)

INIT_STEP = WORDCOUNT_MAP

def execute_next_step(tr, storage_client, job):
    next_step_name = WORDCOUNT_STEPS[job.current_step]["next"]
    WORDCOUNT_STEPS[next_step_name]["callback"](tr, storage_client, job)

