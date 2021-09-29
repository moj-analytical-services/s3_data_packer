import os

def patched_get_filepaths_from_s3_folder(pth: str, file_extension: str=None):
    full_paths = []
    for dirname, _, filenames in os.walk(pth):
        if filenames:
            found_files = [os.path.join(dirname, f) for f in filenames]
            full_paths.extend(found_files)

    if file_extension:
        full_paths = [f for f in full_paths if f.endswith(file_extension)]

    return full_paths
    
def patched_get_s3_file_head(f: str):
    return {"ContentLength": os.stat(f).st_size}
