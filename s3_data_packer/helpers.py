import os
import boto3

from collections import Counter
from dataengineeringutils3 import s3
from glob import glob


def _get_s3_file_head(f: str):
    s3_client = boto3.client("s3")
    bk, ky = s3.s3_path_to_bucket_key(f)
    return s3_client.head_object(Bucket=bk, Key=ky)


def _get_file_size(f: str):
    if f.startswith("s3://"):
        resp = _get_s3_file_head(f)
        file_size_bytes = resp.get("ContentLength", 0)
    else:
        file_size_bytes = os.stat(f).st_size
    return file_size_bytes


def _list_files_in_path(f: str, ext: str = None):
    if f.startswith("s3://"):
        files = s3.get_filepaths_from_s3_folder(f, file_extension=ext)
    else:
        glob_path = os.path.join(f, "**")
        glob_path = os.path.join(glob_path, f"*.{ext}") if ext else glob_path
        files = [f for f in glob(glob_path, recursive=True) if os.path.isfile(f)]

    return files


def get_file_format(pth: str):  # pth has "s3://"" infront
    # are there any files for us to infer from?
    files_in_path = _list_files_in_path(pth)
    if not len(files_in_path):
        return None

    # there are files to infer from, but do they all have the same ff?
    ff_list = [os.path.basename(f).split(".", 1)[-1] for f in files_in_path]
    found_ffs = list(Counter(ff_list).keys())
    if len(found_ffs) != 1:
        raise ValueError(
            "found more than one file format in table path"
            f"in {pth}: {', '.join(found_ffs)}"
        )

    return found_ffs[0]
