import os
import pytest

from dataengineeringutils3 import s3
from helpers import patched_get_filepaths_from_s3_folder, patched_get_s3_file_head
from mojap_metadata import Metadata
from s3_data_packer import s3_output_store
from s3_data_packer.s3_data_packer import S3DataPacker
from shutil import copyfile, rmtree
from tempfile import mkdtemp
# from inspect import getmembers, isfunction
import DataPacker.DataPacker_individual_tests as individual_tests


@pytest.mark.parametrize(
    "function_name",
    [
        "test_append_files",
        "test_set_file_size_on_disk",
        "test_get_latest_file",
        "test_should_append_data",
        "test_data_to_add",
        "test_get_chunk_increments",
        "test_get_meta",
        "test_get_latest_file",
        "test_read_file"
    ]
)
@pytest.mark.parametrize(
    """
    input_file_map,
    output_file_map,
    expected_output_files,
    file_size_limit_in_gb,
    file_size_on_disk,
    get_chunk_increments,
    input_scenario,
    latest_file,
    should_append_data,
    data_to_add,
    get_meta
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
            # expected_output_files: output files expected in the mocked s3 and
            # it's path
            [
                "db/all_types/all_types_0.snappy.parquet"
            ],
            # file_size_limit_in_gb: output parquet file size limit
            1,
            # File size on disk
            0.000006208,
            # chunk increments
            ([1], [10]),
            # input scenario
            1,
            # Does an output file already exist, 0 indicates no
            0,
            # Should we be appending to existing output file
            False,
            # Is there input data to pack?
            True,
            # meta data for casting
            True
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
            # expected_output_files: output files expected in the mocked s3 and it's
            #  path
            [
                "db/all_types/all_types_0.snappy.parquet",
                "db/all_types/all_types_1.snappy.parquet"
            ],
            # file_size_limit_in_gb: output parquet file size limit
            5 * 10**-6,
            # file size on disk
            0.000006208,
            # chunk size increments
            ([1, 9], [8, 10]),
            # input scenario
            2,
            # Does an output file already exist, 0 indicates no
            0,
            # Should we be appending to existing output file
            False,
            # Is there input data to pack?
            True,
            # meta data for casting
            True
        ),
        # input scenario 3: multiple input files, 1 output file
        (
            # input_file_map: input file map, for adding files to the mock land
            {
                "tests/data/all_types.csv":
                "land/all_types/all_types_a.csv",
                "tests/data/all_types2.csv":
                "land/all_types/all_types_b.csv",
            },
            # output_file_map: output file map, for adding files to mock s3 output
            {},
            # expected_output_files: output files expected in the mocked s3 and
            #  it's path
            [
                "db/all_types/all_types_0.snappy.parquet"
            ],
            # file_size_limit_in_gb: output parquet file size limit
            1,
            # file size on disk
            0.000006883,
            # chunk increments
            ([1], [20]),
            # input scenario
            3,
            # Does an output file already exist, 0 indicates no
            0,
            # Should we be appending to existing output file
            False,
            # Is there input data to pack?
            True,
            # meta data for casting
            True
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
                "tests/data/all_types2.csv":
                "land/all_types/all_types_b.csv",
            },
            # output_file_map: output file map, for adding files to mock s3 output
            {},
            # expected_output_files: output files expected in the mocked s3 and
            #  it's path
            [
                "db/all_types/all_types_0.snappy.parquet",
                "db/all_types/all_types_1.snappy.parquet",
            ],
            # file_size_limit_in_gb: output parquet file size limit
            5 * 10**-6,
            # file size on disk
            0.000006883,
            # file chunk increments
            ([1, 15], [14, 20]),
            # input scenario
            4,
            # Does an output file already exist, 0 indicates no
            0,
            # Should we be appending to an existing output file
            False,
            # Is there input data to pack?
            True,
            # meta data for casting
            True
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
            # expected_output_files: output files expected in the mocked s3
            #  and it's path
            [
                "db/all_types/all_types_0.snappy.parquet",
                "db/all_types/all_types_1.snappy.parquet",
            ],
            # file_size_limit_in_gb: output parquet file size limit
            5 * 10**-6,
            # file size on disk
            0.000006883,
            # chunk increments
            ([1, 15], [14, 20]),
            # input scenario
            5,
            # existing output file folder index
            5,
            # Should we append to existing output file
            True,
            # Is there input data to pack?
            True,
            # meta data for casting
            None
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
            # expected_output_files: output files expected in the mocked s3
            #  and it's path
            [
                "db/all_types/all_types_0.snappy.parquet"
            ],
            # file_size_limit_in_gb: output parquet file size limit
            1,
            # file size on disk
            0.000006883,
            # increment chunk size
            ([1], [20]),
            # input scenario
            6,
            # existing output file folder index
            6,
            # Should we append to existing output file
            True,
            # Is there input data to pack?
            True,
            # meta data for casting
            None
        ),
        # input scenario 7: 1 input file, 2 output files (existing files,
        #  but under limit)
        (
            # input_file_map: input file map, for adding files to the mock land
            {
                "tests/data/all_types2.csv":
                "land/all_types/all_types_a.csv",
            },
            # output_file_map: output file map, for adding files to mock s3 output
            {
                "tests/data/all_types.snappy.parquet":
                "db/all_types/all_types_0.snappy.parquet",
                "tests/data/all_types3.snappy.parquet":
                "db/all_types/all_types_3.snappy.parquet",
            },
            # expected_output_files: output files expected in the mocked s3
            #  and it's path
            [
                "db/all_types/all_types_3.snappy.parquet"
            ],
            # file_size_limit_in_gb: output parquet file size limit
            1,
            # file size on disk
            0.000006918,
            # increment chunk size
            ([1], [30]),
            # Input scenario
            7,
            # existing output file folder index
            7,
            # Should we append to existing output file
            True,
            # Is there input data to pack?
            True,
            # meta data for casting
            None
        ),
        # input scenario 8: No input files, no output files.
        # checking everything fails as expected
        (
            # input_file_map: input file map, for adding files to the mock land
            {},
            # output_file_map: output file map, for adding files to mock s3 output
            {},
            # expected_output_files: output files expected in the mocked s3
            #  and it's path
            [],
            # file_size_limit_in_gb: output parquet file size limit
            1,
            # file size on disk
            None,
            # increment chunk size
            None,
            # Input scenario
            8,
            # existing output file folder index
            8,
            # Should we append to existing output file
            False,
            # Is there input data to pack?
            False,
            # meta data for casting
            None
        )
    ]
)
@pytest.mark.parametrize(
    "input_basepath,output_basepath",
    [("land/", "db/")]
)
def test_S3DataPacker(
    monkeypatch, function_name, input_file_map, output_file_map,
    expected_output_files, file_size_limit_in_gb,
    file_size_on_disk, get_chunk_increments,
    input_scenario, latest_file, should_append_data, data_to_add,
    get_meta, input_basepath, output_basepath
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

    # read in individual test functions
    # function_list = [f[1] for f in getmembers(individual_tests, isfunction)]
    f = getattr(individual_tests, function_name)
    # invoke
    # for f in function_list:
    f(pp, ifm=input_file_map, fsod=file_size_on_disk,
      gci=get_chunk_increments,
      inp_s=input_scenario, lf=latest_file,
      sad=should_append_data, dta=data_to_add,
      get_meta=get_meta, meta=meta)

    rmtree(tmp_dir)
