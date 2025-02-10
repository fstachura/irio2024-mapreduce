from google.cloud import storage

# Location is of the form "bucket_name:file_path".
def get_file_handles_from_gstorage(locationsList):
    storage_client = storage.Client()

    file_handles = []
    for location in locationsList:
        bucket_name, file_path = location.split(':')
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_path)

        file_handles.append(blob)

    return file_handles

CODE_CACHE = {}

def save_code_to_cache(location):
    if location not in CODE_CACHE:
        [output_handle] = get_file_handles_from_gstorage([location])
        with output_handle.open('r') as f:
            code = f.read()
            module = globals().copy()
            exec(code, locals=module, globals=module)
            CODE_CACHE[location] = module
