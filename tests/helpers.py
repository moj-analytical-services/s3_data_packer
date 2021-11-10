import os

from arrow_pd_parser import reader, writer
from mojap_metadata import Metadata
from pandas import concat, DataFrame
from pandas.core.frame import DataFrame
from s3_data_packer import s3_output_store
from s3_data_packer.s3_data_packer import S3DataPacker


def setup_packer(
    tmp, input_file_map: dict = None, output_file_map: dict = None, **kwargs
) -> S3DataPacker:

    input_basepath = "land/"
    output_basepath = "db/"

    # join tmp_dir and the basepaths
    output_basepath = os.path.join(tmp, output_basepath)
    input_basepath = os.path.join(tmp, input_basepath)

    # write any required inputs and outputs
    if input_file_map:
        write_file_map(tmp, input_file_map)
    if output_file_map:
        write_file_map(tmp, output_file_map)
    # setup parquet packer
    pp = S3DataPacker(input_basepath, output_basepath, "all_types", **kwargs)

    return pp


def setup_output_store(
    tmp, file_map: dict = None, file_limit_gigabytes: int = 1, table_suffix: str = None
):

    basepath = "db/"

    # join tmp_dir and the basepaths
    basepath = os.path.join(tmp, basepath)

    # write any required existing files
    if file_map:
        write_file_map(tmp, file_map)
    # setup S3OutputStore
    output_store = s3_output_store.S3OutputStore(
        basepath,
        table_name="all_types",
        file_limit_gigabytes=file_limit_gigabytes,
        table_suffix=table_suffix,
    )

    return output_store


def write_file_map(base_dir: str, file_map: dict):
    for source_file, out_files in file_map.items():
        for out_file in out_files:
            dst = os.path.join(base_dir, out_file)
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            writer.write(reader.read(source_file), dst)


def data_maker(
    num_lines: int, source_file: str = "tests/data/all_types.csv", metadata=None
) -> DataFrame:
    # if metadata, get it!
    metadata = Metadata.from_json(metadata) if metadata else metadata
    # get the source file
    source_df = reader.read(source_file, metadata=metadata)
    # how many of these dfs do we need?
    dfs_needed = int(num_lines / len(source_df))
    extra_lines = (
        abs((len(source_df) * dfs_needed) % num_lines) if dfs_needed else num_lines
    )
    # get the correct number of dfs:
    dfs = [source_df for _ in range(dfs_needed)]
    # get any extra lines to cat:
    if extra_lines:
        dfs.append(source_df[:extra_lines])
    # cat them all and return!
    return concat(dfs)
