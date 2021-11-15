import os
import pytest

from helpers import setup_output_store


@pytest.mark.parametrize(
    """
    output_file_map,
    expected_filename,
    """,
    [  # scenario 1: no existing output
        (
            {},
            "all_types_0",
        ),
        # scenario 2: existing output non 0
        (
            {"tests/data/all_types.csv": ["db/all_types/all_types_2"]},
            "all_types_2",
        ),
        # scenario 3: existing output 0
        (
            {"tests/data/all_types.csv": ["db/all_types/all_types_0"]},
            "all_types_0",
        ),
        # scenario 4 many existing output non 0, non sequential
        (
            {
                "tests/data/all_types.csv": [
                    "db/all_types/all_types_0",
                    "db/all_types/all_types_12",
                ]
            },
            "all_types_12",
        ),
    ],
)
@pytest.mark.parametrize("file_format", ["csv", "jsonl", "parquet", "snappy.parquet"])
@pytest.mark.parametrize("table_suffix", [None, "a_suffix", "55", "uwu"])
@pytest.mark.parametrize("full_path", [True, False])
def test_get_filename(
    tmp_path, output_file_map, file_format, expected_filename, table_suffix, full_path
):
    """
    _get_latest_file_by_suffix and _get_filename both return the same thing so can be
    tested with the same paremeters except for that _get_latest_file_by_suffix orders
    them according to a paremeter
    It will test that the most recent file is returned
    """

    # put the file format on the output store, as it isn't there already
    output_file_map = {
        k: [f"{f}.{file_format}" for f in v] for k, v in output_file_map.items()
    }
    # setup output_store
    ps = setup_output_store(tmp_path, output_file_map, table_suffix=table_suffix)
    ps.table_extension = file_format
    # build expected filename
    expected_filename = (
        f"_{table_suffix}_".join(expected_filename.rsplit("_", 1))
        if table_suffix
        else expected_filename
    )
    expected_filename = f"{expected_filename}.{file_format}"
    expected_filename = (
        os.path.join(tmp_path, "db/all_types", expected_filename)
        if full_path
        else expected_filename
    )
    # assert matches expected output
    assert ps._get_filename(full_path=full_path) == expected_filename


@pytest.mark.parametrize(
    "fn,expected_num", [("a_file_0", 0), ("a_file_10", 10), ("a_file_1984_5", 5)]
)
@pytest.mark.parametrize("ff", ["csv", "jsonl", "parquet", "snappy.parquet"])
def test_get_filenum_from_filename_pass(tmp_path, fn, ff, expected_num):
    ps = setup_output_store(tmp_path)
    assert ps._get_filenum_from_filename(f"{fn}.{ff}") == expected_num


@pytest.mark.parametrize("fn", ["a_file.csv", "another_file_0322_asfas.csv", 0, None])
def test_get_filenum_from_filename_fail(tmp_path, fn):
    ps = setup_output_store(tmp_path)
    with pytest.raises(ValueError):
        ps._get_filenum_from_filename(fn)


@pytest.mark.parametrize(
    """output_file_map,expected_output_store_filenum""",
    [
        ({}, 0),
        ({"tests/data/all_types.csv": ["db/all_types/at_0.snappy.parquet"]}, 0),
        ({"tests/data/all_types.csv": ["db/all_types/at_5.snappy.parquet"]}, 5),
        (
            {
                "tests/data/all_types.csv": [
                    "db/all_types/at_0.snappy.parquet",
                    "db/all_types/at_7.snappy.parquet",
                ]
            },
            7,
        ),
        (
            {
                "tests/data/all_types.csv": [
                    "db/all_types/at_0.snappy.parquet",
                    "db/all_types/at_1.snappy.parquet",
                ]
            },
            1,
        ),
        (
            {
                "tests/data/all_types.csv": [
                    "db/all_types/at_5.snappy.parquet",
                    "db/all_types/at_7456.snappy.parquet",
                ]
            },
            7456,
        ),
    ],
)
def test_output_store_filenum(tmp_path, output_file_map, expected_output_store_filenum):
    output_store = setup_output_store(tmp_path, file_map=output_file_map)
    assert output_store.filenum == expected_output_store_filenum


@pytest.mark.parametrize(
    "file_list",
    [
        ["all_types_0.csv", "all_types_1.csv"],
        ["all_types_0.csv", "all_types_1.csv", "all_types_2.csv"],
        ["all_types_5.csv", "all_types_50.csv"],
        ["all_types_0.csv"],
        ["all_types_3.csv"],
        [],
    ],
)
@pytest.mark.parametrize("maxim", [True, False])
@pytest.mark.parametrize("full_path", [True, False])
def test_get_latest_file_by_suffix(tmp_path, file_list, maxim, full_path):

    # setup
    output_store = setup_output_store(tmp_path)
    # get latest file from given list
    latest_file = output_store._get_latest_file_by_suffix(
        file_list, maxim=maxim, full_path=full_path
    )

    # determine expected file
    expected_index = -1 if maxim else 0
    expected_file = file_list[expected_index] if file_list else None
    expected_file = (
        os.path.join(tmp_path, "db/all_types", expected_file)
        if (full_path and file_list)
        else expected_file
    )

    # assert!
    assert expected_file == latest_file


@pytest.mark.parametrize(
    "output_file_map",
    [  # no existing output
        ({}),
        # existing output
        ({"tests/data/all_types.csv": ["db/all_types/all_types_0.snappy.parquet"]}),
    ],
)
@pytest.mark.parametrize("file_limit_gigabytes", [1, 1 * 10 ** -6])
def test_should_append_data(tmp_path, output_file_map, file_limit_gigabytes):
    output_store = setup_output_store(
        tmp_path, file_map=output_file_map, file_limit_gigabytes=file_limit_gigabytes
    )

    # figure out if you should append the data:
    should_append = True if (output_file_map and file_limit_gigabytes == 1) else False

    assert should_append == output_store._should_append_data()
