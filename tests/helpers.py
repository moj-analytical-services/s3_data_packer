import os
from dataengineeringutils3 import s3
from mojap_metadata import Metadata
from s3_data_packer import s3_output_store
from s3_data_packer.s3_data_packer import S3DataPacker
from shutil import copyfile
from tempfile import mkdtemp


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


def setup_packer(monkeypatch, input_file_map, output_file_map,
                 file_size_limit_in_gb) -> S3DataPacker:

    input_basepath = "land/"
    output_basepath = "db/"

    # make the tmp dir for all data to go
    tmp_dir = mkdtemp()

    # join tmp_dir and the basepaths
    output_basepath = os.path.join(tmp_dir, output_basepath)
    input_basepath = os.path.join(tmp_dir, input_basepath)

    # patch out dataengineeringutils3.s3.get_filepaths_from_s3_folder
    monkeypatch.setattr(
        s3, "get_filepaths_from_s3_folder", patched_get_filepaths_from_s3_folder
    )

    # patch out S3OutputStore _get_s3_file_head
    monkeypatch.setattr(s3_output_store, "_get_s3_file_head", patched_get_s3_file_head)

    # write any input files to input_basepath
    for source_file, out_file in input_file_map.items():
        dst = os.path.join(tmp_dir, out_file)
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        copyfile(source_file, dst)
    # write any pre existing output to output_basepath
    for source_file, out_file in output_file_map.items():
        dst = os.path.join(tmp_dir, out_file)
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        copyfile(source_file, dst)

    # get the metadata
    meta = Metadata.from_json("tests/data/all_types.json")
    # setup parquet packer
    pp = S3DataPacker(input_basepath, output_basepath, "all_types", meta)
    # set the output store file limit
    pp.output_store.parquet_file_limit_gigabytes = file_size_limit_in_gb

    return pp
