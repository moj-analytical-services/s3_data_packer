import os
import pytest
import tempfile

import pandas as pd

from arrow_pd_parser import writer, reader
from pandas.testing import assert_frame_equal
from mojap_metadata.metadata.metadata import Metadata
from arrow_pd_parser.caster import cast_pandas_table_to_schema
from helpers import setup_packer, data_maker


@pytest.mark.parametrize(
    """
    file_format,expected_get_meta_return_type,cast_parquet
    """,
    [
        # parquet inputs: returns metadata only if cast_parquet is true
        ("parquet", Metadata, True),
        ("parquet", type(None), False),
        ("snappy.parquet", Metadata, True),
        ("snappy.parquet", type(None), False),
        # jsonl/csv inputs: always returns metadata regardless of cast_parquet
        ("csv", Metadata, True),
        ("csv", Metadata, False),
        ("jsonl", Metadata, True),
        ("jsonl", Metadata, False),
    ],
)
def test_get_meta(
    tmp_path, file_format: str,
    expected_get_meta_return_type: bool, cast_parquet: bool
):
    # set up
    pp = setup_packer(
        tmp_path, cast_parquet=cast_parquet,
        metadata="tests/data/all_types.json"
    )
    # assert type is expected type
    assert type(pp._get_meta(file_format)) == expected_get_meta_return_type


@pytest.mark.parametrize(
    """
    input_file_map,
    output_file_map,
    file_limit_gigabytes,
    expected_output_df
    """,
    [   
        # scenario 1: no existing data, and the input data is all appended
        (
            {
                "tests/data/all_types.csv":[
                    "land/all_types/all_types.csv",
                    "land/all_types/all_types2.csv",
                ]
            },
            {},
            1,
            data_maker(20)
        ),
        # scenario 2: existing data, and the data is appended to the end of the file
        (
            {
                "tests/data/all_types.csv":[
                    "land/all_types/all_types.csv",
                    "land/all_types/all_types2.csv",
                ]
            },
            {"tests/data/all_types.csv":["db/all_types/all_types_0.snappy.parquet"]},
            1,
            data_maker(30)
        ),
        # scenatio 3: existing data, and the data is not appened to the end
        # the input data is about ~8kb on disk, so the file limit is set to 6kb
        (
            {
                "tests/data/all_types.csv":[
                    "land/all_types/all_types.csv",
                    "land/all_types/all_types2.csv",
                ]
            },
            {"tests/data/all_types.csv":["db/all_types/all_types_0.snappy.parquet"]},
            6*10**-6,
            data_maker(20)
        )
    ]
)
@pytest.mark.parametrize(
    "metadata",[None, Metadata.from_json("tests/data/all_types.json")]
)
def test_append_files(
    tmp_path, input_file_map, output_file_map,
    file_limit_gigabytes, expected_output_df, metadata
):
    # positive tests for appending files i.e. ones that won't fail
    pp = setup_packer(
        tmp_path, input_file_map, output_file_map, 
        file_limit_gigabytes=file_limit_gigabytes, metadata=metadata, cast_parquet=True
    )
    pp.output_store.table_extension = "snappy.parquet"
    expected_output_df = cast_pandas_table_to_schema(
        expected_output_df, metadata
    ) if metadata else expected_output_df
    assert_frame_equal(pp._append_files(), expected_output_df)


@pytest.mark.parametrize("data_size",[1, 3, 10, 100, 1000, 10000])
# No jsonl as there are issues with pandas and jsonl
@pytest.mark.parametrize("data_format", ["csv", "parquet", "snappy.parquet"])
@pytest.mark.parametrize("metadata", [True, False])
def test_set_file_size_on_disk(tmp_path, data_size, data_format, metadata):
    # get metadata if required
    metadata = "tests/data/all_types.json" if metadata else None
    # setup packer
    pp = setup_packer(tmp_path, metadata=metadata)
    pp.output_store.table_extension = data_format
    # get the dataframe
    df = data_maker(data_size, metadata=metadata)
    # write to disk and get size from os.path.getsize
    with tempfile.NamedTemporaryFile(suffix = f".{data_format}") as f:
        writer.write(df, f.name)
        expected_file_size = round(os.path.getsize(f.name) * 10**-9, 9)
    # get the file size on disk according to s3_data_packer
    pp._set_file_size_on_disk(df)
    # asssert!
    assert round(pp.file_size_on_disk, 9) == expected_file_size


@pytest.mark.parametrize(
    "file_limit_gigabytes, df, expected_chunk_increments",
    [   # input scenario 1: large (ish) input file, small file size limit
        (5 * 10 ** -6, data_maker(120), ([0, 82], [82, 120])),
        # input scenario 2: large file, large file limit
        (1, data_maker(120), ([0], [120])),
        # input scenario 3: one line file
        (1, data_maker(1), ([0], [1]))
    ],
)
def test_get_chunk_increments(
    tmp_path, file_limit_gigabytes, df, expected_chunk_increments,
):
    pp = setup_packer(tmp_path, file_limit_gigabytes=file_limit_gigabytes)
    pp._set_file_size_on_disk(df)
    assert pp._get_chunk_increments() == expected_chunk_increments


@pytest.mark.parametrize(
    """
    input_filemap,output_filemap,total_lines
    """,
    [
        # scenario 0: single line input, no existing output
        (
            {
                "tests/data/all_types_one_line.csv": [
                    "land/all_types/all_types_a"
                ]
            },
            {},
            1
        ),
        # scenario 1: 1 file in, no existing output
        (
            {
                "tests/data/all_types.csv": [
                    "land/all_types/all_types_a"
                ]
            },
            {},
            10
        ),
        # scenario 2: multiple data in, no existing output
        (
            {
                "tests/data/all_types.csv": [
                    "land/all_types/all_types_a",
                    "land/all_types/all_types_b",
                ]
            },
            {},
            20
        ),
        # scenario 3: 1 data in, 1 data existing output
        (
            {
                "tests/data/all_types.csv": [
                    "land/all_types/all_types_a"
                ]
            },
            {
                "tests/data/all_types.csv": [
                    "db/all_types/all_types_0"
                ]
            },
            20
        ),
        # scenario 4: multi in, multi existing
        (
            {
                "tests/data/all_types.csv": [
                    "land/all_types/all_types_a",
                    "land/all_types/all_types_b",
                ]
            },
            {
                "tests/data/all_types.csv": [
                    "db/all_types/all_types_0",
                    "db/all_types/all_types_1",
                ]
            },
            40
        ),
    ]
)
@pytest.mark.parametrize("file_limit_gigabytes", [1, 6*10**-6])
# jsonl isn't done because pandas makes boolean jsonl values ints and it's a pain
@pytest.mark.parametrize("ff", ["csv", "parquet", "snappy.parquet"])
def test_packed_data(
    tmp_path, input_filemap, output_filemap, total_lines, file_limit_gigabytes, ff
):
    """
    tests that the data in is the same as all the data out, regardless of split.
    This is the "end to end" test
    """
    # add the file formats to the files
    if output_filemap:
        output_filemap = {k:[f"{f}.{ff}" for f in v] for k,v in output_filemap.items()}
    if input_filemap:
        input_filemap = {k:[f"{f}.{ff}" for f in v] for k,v in input_filemap.items()}

    pp = setup_packer(
        tmp_path, input_filemap, output_filemap,
        file_limit_gigabytes=file_limit_gigabytes, output_file_ext=ff
    )
    pp.pack_data()
    output_basepath = os.path.join(tmp_path, "db/all_types/")
    out_files = [os.path.join(output_basepath, f) for f in os.listdir(output_basepath)]
    df = pd.concat([reader.read(f) for f in out_files])

    """ two things are going to happen here:
        1. index is reset, this is so that the indexes aren't counted in whats different
           this is done because by default, index data is kept, but in this context it
           isn't important
        2. sorting by the i column, as when the data is split up across multiple files
           it changes the order of things sometimes, this is an issue. We want to
           determine whether the same data ended up in the output as we put in, we don't
           care about the order in which it appears in the file
    """
    assert_frame_equal(
        df.sort_values("i").reset_index(drop=True),
        data_maker(total_lines).sort_values("i").reset_index(drop=True)
    )
