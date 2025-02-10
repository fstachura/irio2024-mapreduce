import datetime
import uuid
import threading
from math import ceil
from google.cloud import storage
from sqlalchemy import select

from .database import Job, JobPart

def split_on_same_elements(l, parts, eq):
    if parts == 1:
        return [l]

    result = []
    part_len = ceil(len(l)/parts)
    part_end_idxes = [(n+1)*part_len for n in range(parts-1)]

    for n in range(parts-1):
        i = part_end_idxes[n]
        while i < len(l) and eq(l[i-1], l[i]):
            i += 1
        part_end_idxes[n] = i

    start = 0
    for idx in part_end_idxes:
        result.append(l[start:idx])
        start = idx

    result.append(l[part_end_idxes[-1]:])
    return result

def generate_tmp_location(base, step):
    return base.rstrip("/") + f"_tmp/{step}/" + str(uuid.uuid4())

def get_blob(location):
    storage_client = storage.Client()
    bucket_name, filename = location.split(':')
    bucket = storage_client.bucket(bucket_name)
    return bucket.get_blob(filename)

def generate_upload_url(bucket, filename):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket)
    blob = bucket.blob(filename)

    url = blob.generate_signed_url(
        version="v4",
        expiration=datetime.timedelta(hours=1),
        method="PUT",
        content_type="application/octet-stream"
    )

    return url

def get_unfinished_job_parts(tr):
    stmt = select(JobPart).where(JobPart.finished == False)
    return tr.execute(stmt).scalars().all()

def get_unfinished_job(tr):
    stmt = select(Job).where(Job.finished == False)
    return tr.execute(stmt).scalars().first()

class AtomicInt:
    def __init__(self, init):
        self.cnt = init
        self.lock = threading.Lock()

    def set(self, val):
        with self.lock:
            self.cnt = val

    def get(self):
        with self.lock:
            return self.cnt

def format_uuid(uid):
    return str(uid)[:8]


CODE_CACHE = {}

def save_code_to_cache(location):
    if location not in CODE_CACHE:
        with get_blob(location).open('r') as f:
            code = f.read()
            module = globals().copy()
            exec(code, locals=module, globals=module)
            CODE_CACHE[location] = module

def init_custom_algorithm(location, tr, storage_client, job):
    save_code_to_cache(location)
    steps = CODE_CACHE[location]["ALGORITHM_STEPS"]
    init = CODE_CACHE[location]["INIT_STEP"]
    steps[init]["callback"](tr, storage_client, job)

def execute_next_custom_step(location, tr, storage_client, job):
    save_code_to_cache(location)
    steps = CODE_CACHE[location]["ALGORITHM_STEPS"]
    next_step_name = steps[job.current_step]["next"]
    steps[next_step_name]["callback"](tr, storage_client, job)

