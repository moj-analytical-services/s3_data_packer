import os
import pytest

from dataengineeringutils3 import s3
from helpers import patched_get_filepaths_from_s3_folder, patched_get_s3_file_head
from mojap_metadata import Metadata
from s3_data_packer import s3_output_store
from s3_data_packer.s3_data_packer import S3DataPacker
from shutil import copyfile, rmtree
from tempfile import mkdtemp

@pytest.mark.parametrize(
    """
    input_file_map,
    output_file_map,
    expected_output_files,
    file_size_limit_in_gb
    """, 
    [
        # input scenario 1: 1 input file, 1 output file, no existing data
        (
            # input_file_map: input file map, for adding files to the mock land
            {
                "tests/data/all_types.csv":
                "land/all_types/all_types.csv",
            },
            # output_file_map: output file map, for adding files to mock s3 output
            {},
            # expected_output_files: output files expected in the mocked s3 and it's path
            [
                "db/all_types/all_types_0.snappy.parquet"
            ],
            # file_size_limit_in_gb: output parquet file size limit
            1
        ),
        # input scenario 2: 1 input file, multiple output files
        (
            # input_file_map: input file map, for adding files to the mock land
            {
                "tests/data/all_types.csv":
                "land/all_types/all_types.csv",
            },
            # output_file_map: output file map, for adding files to mock s3 output
            {},
            # expected_output_files: output files expected in the mocked s3 and it's path
            [
                "db/all_types/all_types_0.snappy.parquet",
                "db/all_types/all_types_1.snappy.parquet"
            ],
            # file_size_limit_in_gb: output parquet file size limit
            5*10**-6
        ),
        # input scenario 3: multiple input files, 1 output file
        (
            # input_file_map: input file map, for adding files to the mock land
            {
                "tests/data/all_types.csv":
                "land/all_types/all_types_a.csv",
                "tests/data/all_types.csv":
                "land/all_types/all_types_b.csv",
            },
            # output_file_map: output file map, for adding files to mock s3 output
            {},
            # expected_output_files: output files expected in the mocked s3 and it's path
            [
                "db/all_types/all_types_0.snappy.parquet"
            ],
            # file_size_limit_in_gb: output parquet file size limit
            1
        ),
        # input scenario 4: multiple input files, multiple output files. This is 
        # expected to have two outputs, even though there are more inputs due to snappy
        # compression in parquet working on repeated data see
        # https://dsdmoj.atlassian.net/browse/CCDE-146 for demonstration of the
        # compression on repeated vs random data
        (
            # input_file_map: input file map, for adding files to the mock land
            {
                "tests/data/all_types.csv":
                "land/all_types/all_types_a.csv",
                "tests/data/all_types.csv":
                "land/all_types/all_types_b.csv",
            },
            # output_file_map: output file map, for adding files to mock s3 output
            {},
            # expected_output_files: output files expected in the mocked s3 and it's path
            [
                "db/all_types/all_types_0.snappy.parquet",
                "db/all_types/all_types_1.snappy.parquet",
            ],
            # file_size_limit_in_gb: output parquet file size limit
            5*10**-6
        ),
        # input scenario 5: 1 input file, 2 output files due to existing file in output
        (
            # input_file_map: input file map, for adding files to the mock land
            {
                "tests/data/all_types.csv":
                "land/all_types/all_types_a.csv",
            },
            # output_file_map: output file map, for adding files to mock s3 output
            {
                "tests/data/all_types.snappy.parquet":
                "db/all_types/all_types_0.snappy.parquet"
            },
            # expected_output_files: output files expected in the mocked s3 and it's path
            [
                "db/all_types/all_types_0.snappy.parquet",
                "db/all_types/all_types_1.snappy.parquet",
            ],
            # file_size_limit_in_gb: output parquet file size limit
            5*10**-6
        ),
        # input scenario 6: 1 input file, 1 output file (existing file, but under limit)
        (
            # input_file_map: input file map, for adding files to the mock land
            {
                "tests/data/all_types.csv":
                "land/all_types/all_types_a.csv",
            },
            # output_file_map: output file map, for adding files to mock s3 output
            {
                "tests/data/all_types.snappy.parquet":
                "db/all_types/all_types_0.snappy.parquet"
            },
            # expected_output_files: output files expected in the mocked s3 and it's path
            [
                "db/all_types/all_types_0.snappy.parquet"
            ],
            # file_size_limit_in_gb: output parquet file size limit
            1
        )
    ]
)
@pytest.mark.parametrize(
    "input_basepath,output_basepath",
    [("land/","db/")]
)
def test_packer_end_to_end(
    monkeypatch, input_file_map, output_file_map, expected_output_files,
    file_size_limit_in_gb, input_basepath, output_basepath
):
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
        os.makedirs(os.path.dirname(dst))
        copyfile(source_file, dst)
    # write any pre existing output to output_basepath
    for source_file, out_file in output_file_map.items():
        dst = os.path.join(tmp_dir, out_file)
        os.makedirs(os.path.dirname(dst))
        copyfile(source_file, dst)

    # get the metadata
    meta = Metadata.from_json("tests/data/all_types.json")
    # setup parquet packer
    pp = S3DataPacker(input_basepath, output_basepath, "all_types", meta)
    # set the output store file limit
    pp.output_store.parquet_file_limit_gigabytes=file_size_limit_in_gb
    # pack 'em
    pp.pack_data()
    # list all the files in the output location
    # output_files = os.listdir(os.path.join(tmp_dir, output_basepath))

    output_files = []
    for dirname, _, filenames in os.walk(os.path.join(tmp_dir, output_basepath)):
        if filenames:
            found_files = [os.path.join(dirname, f) for f in filenames]
            output_files.extend(found_files)
    # get the full path of the expected output  files
    expected_output_files = [
        os.path.join(tmp_dir, f) for f in expected_output_files
    ]
    # assert they are the same
    error = None
    try:
        assert set(output_files) == set(expected_output_files)
    except Exception as e:
        error = e
    finally:
        rmtree(tmp_dir)
        if error:
            raise error
