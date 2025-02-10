import logging
import string
from math import ceil
from sqlalchemy import select

from .database import JobPart
from .utils import split_on_same_elements, generate_tmp_location, format_uuid

logger = logging.getLogger(__name__)

WORDCOUNT_MAP = "map"
WORDCOUNT_SHUFFLE = "shuffle"
WORDCOUNT_REDUCE = "reduce"
WORDCOUNT_COLLECT = "collect"

PUNCTUATION_CHARACTERS = string.punctuation + string.whitespace
SMALL_FILE_SIZE = 128*1024

def start_map(tr, storage_client, job):
    bucket_name, directory_name = job.input_location.split(':')
    bucket = storage_client.bucket(bucket_name)
    if directory_name[-1] != "/":
        directory_name += "/"

    parts = []
    for blob in bucket.list_blobs(prefix=directory_name):
        if blob.name.rstrip("/") != directory_name.rstrip("/"):
            logger.info(f"starting map, processing file of job {format_uuid(job.job_uuid)}")

            if blob.size < SMALL_FILE_SIZE or job.expected_parts == 1:
                logger.info(f"file is small ({blob.size} bytes), processing whole")
                parts.append((blob.name, 0, blob.size))
            else:
                part_size = ceil(blob.size / job.expected_parts)
                range_start, range_end = 0, part_size
                with blob.open('r') as f:
                    for _ in range(job.expected_parts):
                        if range_start >= blob.size:
                            break

                        f.seek(range_end)
                        while range_end <= blob.size:
                            char = f.read(1)
                            range_end += len(char.encode())
                            if char in PUNCTUATION_CHARACTERS:
                                break

                        if range_end > blob.size:
                            range_end = blob.size

                        if range_end - range_start <= 0:
                            break

                        logger.info(f"dividing file into parts {range_start}-{range_end} of {blob.size}")
                        parts.append((blob.name, range_start, range_end))
                        range_start = range_end
                        range_end += part_size

    for name, start, end in parts:
        job_part = JobPart(input_location=f"{bucket_name}:{name}",
                       range_start=start,
                       range_end=end,
                       finished=False,
                       job_id=job.id,
                       step=WORDCOUNT_MAP)
        tr.add(job_part)

    tr.commit()

def execute_shuffle(tr, storage_client, job):
    stmt = select(JobPart).where(JobPart.step == WORDCOUNT_MAP, JobPart.job_id == job.id)
    parts = tr.execute(stmt).scalars().all()
    result = []

    logger.info(f"starting shuffle of job {format_uuid(job.job_uuid)}")

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
        size = 0
        with job_bucket.blob(input_location).open('w') as f:
            for part in reduce_part:
                to_write = part[0] + "," + part[1] + "\n"
                f.write(to_write)
                size += len(to_write.encode())

        part = JobPart(input_location=f"{job_bucket_name}:{input_location}",
                       finished=False,
                       job_id=job.id,
                       step=WORDCOUNT_REDUCE,
                       range_start=0,
                       range_end=size)
        tr.add(part)

    job.current_step = WORDCOUNT_REDUCE
    tr.commit()

    logger.info(f"shuffle of job {format_uuid(job.job_uuid)} finished")
    # in a transaction - if coordinator crashes, neither the transition nor job parts get saved

def execute_collect(tr, storage_client, job):
    stmt = select(JobPart).where(JobPart.step == WORDCOUNT_REDUCE, JobPart.job_id == job.id)
    parts = tr.execute(stmt).scalars().all()

    logger.info(f"starting collect of job {format_uuid(job.job_uuid)}")

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

    job.current_step = "collect"
    job.finished = None
    tr.commit()
    logger.info(f"job {format_uuid(job.job_uuid)} finished")


ALGORITHM_STEPS = {
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

INIT_STEP = WORDCOUNT_MAP

def init_algorithm(tr, storage_client, job):
    ALGORITHM_STEPS[INIT_STEP]["callback"](tr, storage_client, job)

def execute_next_step(tr, storage_client, job):
    next_step_name = ALGORITHM_STEPS[job.current_step]["next"]
    ALGORITHM_STEPS[next_step_name]["callback"](tr, storage_client, job)

