import os
import pytest

from tests.helpers import setup_output_store, setup_input_store


@pytest.mark.parametrize(
    """
    output_file_map,
    expected_filename,
    partition
    """,
    [  # scenario 1: no existing output
        ({}, "all_types_0", None),
        # scenario 2: existing output non 0
        (
            {"tests/data/all_types.csv": ["db/all_types/all_types_2"]},
            "all_types_2",
            None,
        ),
        # scenario 3: existing output 0
        (
            {"tests/data/all_types.csv": ["db/all_types/all_types_0"]},
            "all_types_0",
            None,
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
            None,
        ),
        # scenario 5 existing output and partition
        (
            {
                "tests/data/all_types.csv": [
                    "db/all_types/some_val=2020-02-01/all_types_0",
                    "db/all_types/some_val=2004-11-16/all_types_1",
                ]
            },
            "all_types_1",
            {"some_val": "2004-11-16"},
        ),
        # existing output and parition, but picks lower numbered one because of
        # partition picked
        (
            {
                "tests/data/all_types.csv": [
                    "db/all_types/some_val=2020-02-01/all_types_0",
                    "db/all_types/some_val=2004-11-16/all_types_1",
                ]
            },
            "all_types_0",
            {"some_val": "2004-02-01"},
        ),
    ],
)
@pytest.mark.parametrize("file_format", ["csv", "jsonl", "parquet", "snappy.parquet"])
@pytest.mark.parametrize("table_suffix", [None, "a_suffix", "55", "uwu"])
@pytest.mark.parametrize("full_path", [True, False])
def test_get_filename(
    tmp_path,
    output_file_map,
    file_format,
    expected_filename,
    table_suffix,
    full_path,
    partition,
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
    ps = setup_output_store(
        tmp_path, output_file_map, table_suffix=table_suffix, partition=partition
    )
    ps.table_extension = file_format
    # build expected filename
    expected_filename = (
        f"_{table_suffix}_".join(expected_filename.rsplit("_", 1))
        if table_suffix
        else expected_filename
    )
    expected_filename = f"{expected_filename}.{file_format}"
    if partition is not None and full_path:
        p_name, p_val = list(partition.items())[0]
        expected_filename = os.path.join(f"{p_name}={p_val}", expected_filename)
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


def test_input_partitions(tmp_path):
    """
    tests that the partition values are succesfully captured.
    """

    inp_fm = {
        "tests/data/all_types.csv": [
            "db/all_types/some_val=1962-10-28/at_0.snappy.parquet",
            "db/all_types/some_val=fully_string/at_0.snappy.parquet",
            "db/all_types/some_val=True/at_0.snappy.parquet",
            "db/all_types/some_val=129531/at_0.snappy.parquet",
            "db/all_types/some_val=!@£$%^&*()_+-';,.<>[]~`/at_0.snappy.parquet",
        ]
    }
    expected_partition_values = [
        "1962-10-28",
        "fully_string",
        "True",
        "129531",
        "!@£$%^&*()_+-';,.<>[]~`",
    ]
    expected_partition_values.sort()

    input_store = setup_input_store(tmp_path, inp_fm, basepath="db/")
    input_store.partition_name = "some_val"
    input_store.partition_values.sort()
    assert input_store.partition_values == expected_partition_values


@pytest.mark.parametrize(
    "output_file_map,partition,expected_latest_file",
    [
        # scenario 1: it picks the correct latest file, givent the partition
        (
            {
                "tests/data/all_types.csv": [
                    "db/all_types/some_val=1/all_types_1.snappy.parquet",
                    "db/all_types/some_val=1/all_types_2.snappy.parquet",
                    "db/all_types/some_val=2/all_types_3.snappy.parquet",
                ]
            },
            {"some_val": "2"},
            "all_types_3.snappy.parquet",
        ),
        # secenario 2: basic usage, one partiion, one file
        (
            {
                "tests/data/all_types.csv": [
                    "db/all_types/some_val=1/all_types_0.snappy.parquet",
                ]
            },
            {"some_val": "1"},
            "all_types_0.snappy.parquet",
        ),
        # scenario 3: no existing output files
        ({}, {"some_val": "2"}, "all_types_0.snappy.parquet"),
        # secenario 4: existing output, but not in the partition
        (
            {
                "tests/data/all_types.csv": [
                    "db/all_types/some_val=2/all_types_1.snappy.parquet",
                ]
            },
            {"some_val": "1"},
            "all_types_0.snappy.parquet",
        ),
    ],
)
def test_latest_file_with_partition(
    tmp_path, output_file_map, partition, expected_latest_file
):
    output_store = setup_output_store(tmp_path, output_file_map, partition=partition)
    assert output_store._get_filename() == expected_latest_file
