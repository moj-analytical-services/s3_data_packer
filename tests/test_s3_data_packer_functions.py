from arrow_pd_parser import reader
import pytest
from pandas.testing import assert_frame_equal
from pandas import concat
from mojap_metadata.metadata.metadata import Metadata
from arrow_pd_parser.caster import cast_pandas_table_to_schema
from helpers import setup_packer
from tests.whatnow import append_files


@pytest.mark.parametrize(
    """
    input_file_map,
    output_file_map,
    file_size_limit_in_gb,
    input_scenario
    """,
    [       # input scenario 1: plenty of input, no existing output
        (   # dictionary specifying files to copy over to mock land
            {"tests/data/append_files/input_scenario_1/all_types.csv":
             "land/all_types/all_types.csv",
             "tests/data/append_files/input_scenario_1/all_types2.csv":
             "land/all_types/all_types2.csv",
             "tests/data/append_files/input_scenario_1/all_types3.csv":
             "land/all_types/all_types3.csv"
             },
            # Not stipulating any extant output
            {},
            # Setting file size limit for later divvying up
            1,
            # input_scenario
            1
        ),
        (   # input scenario 2: plenty of input, plenty of existing output
            # dictionary specifying files to copy over to mock land
            {"tests/data/append_files/input_scenario_2/all_types.csv":
             "land/all_types/all_types.csv",
             "tests/data/append_files/input_scenario_2/all_types2.csv":
             "land/all_types/all_types2.csv",
             "tests/data/append_files/input_scenario_2/all_types3.csv":
             "land/all_types/all_types3.csv"
             },
            # existing output
            {"tests/data/append_files/input_scenario_2/all_types.snappy.parquet":
             "db/all_types/all_types_0.snappy.parquet",
             "tests/data/append_files/input_scenario_2/all_types_2.snappy.parquet":
             "db/all_types/all_types_1.snappy.parquet",
             "tests/data/append_files/input_scenario_2/all_types_3.snappy.parquet":
             "db/all_types/all_types_2.snappy.parquet",
             "tests/data/append_files/input_scenario_2/all_types_4.snappy.parquet":
             "db/all_types/all_types_4.snappy.parquet"
             },
            # Setting file size limit for later divvying up
            1,
            # input_scenario
            2
        ),
        (   # input sceario 3: no input, plenty of existing output
            # dictionary specifying files to copy over to mock land
            {},
            # existing output
            {"tests/data/append_files/input_scenario_3/all_types.snappy.parquet":
             "db/all_types/all_types_0.snappy.parquet",
             "tests/data/append_files/input_scenario_3/all_types_2.snappy.parquet":
             "db/all_types/all_types_1.snappy.parquet"},
            # Setting file size limit for later divvying up
            1,
            # input_scenario
            3
        ),
        (   # input scenario 4: no input, no existing output
            # dictionary specifying files to copy over to mock land
            {},
            # Not stipulating any extant output
            {},
            # Setting file size limit for later divvying up
            1,
            # input_scenario
            4
        )
    ]
)
def test_append_files(monkeypatch, input_file_map, output_file_map,
                      file_size_limit_in_gb, input_scenario):

    pp = setup_packer(monkeypatch, input_file_map, output_file_map,
                      file_size_limit_in_gb)

    # if there are no input files and no existing output files,
    # expecting a value error when we try to append,
    #  as there are no new input files to read in and concatenate.
    if input_scenario == 4:
        with pytest.raises(ValueError):
            appended_files_pp = pp._append_files()

    else:
        appended_files_pp = pp._append_files()
        appended_files_expected = reader.read("tests/data/append_files/input_scenario_"
                                              f"{input_scenario}/appended_output"
                                              ".snappy.parquet")
        assert_frame_equal(appended_files_pp, appended_files_expected)


@pytest.mark.parametrize(
    """
    input_file_map,
    output_file_map,
    file_size_limit_in_gb,
    input_scenario,
    appended_file,
    expected_file_size
    """,
    [       # input scenario 1: large (ish) input file, small file size limit
        (
            {},
            # Not stipulating any extant output
            {},
            # Setting file size limit for later divvying up
            5 * 10**-6,
            # input scenario
            1,
            # appended file
            "tests/data/get_chunk_increments/all_types_x12.csv",
            # expected file size
            0.00000641
        ),
        (   # input scenario 2: one line file
            {},
            # Not stipulating any extant output
            {},
            # Setting file size limit for later divvying up
            1,
            # input scenario
            2,
            # appended file
            "tests/data/get_chunk_increments/all_types_one_line.csv",
            # expected file size
            0.000005778
        ),
        (   # input scenario 3: no input files
            {},
            # Not stipulating any extant output
            {},
            # Setting file size limit for later divvying up
            5 * 10**-6,
            # input scenario
            3,
            # no appended file
            None,
            # no expected file size
            None
        )
    ]
)
def test_set_file_size_on_disk(monkeypatch, input_file_map, output_file_map,
                               file_size_limit_in_gb, input_scenario,
                               appended_file, expected_file_size):

    pp = setup_packer(monkeypatch, input_file_map, output_file_map,
                      file_size_limit_in_gb)

    if input_scenario == 3:
        with pytest.raises(AttributeError):
            pp._set_file_size_on_disk(appended_file)

    else:
        meta = Metadata.from_json("tests/data/all_types.json")
        file = cast_pandas_table_to_schema(
            reader.read(appended_file), meta)

        pp._set_file_size_on_disk(file)
        assert round(pp.file_size_on_disk, 9) == expected_file_size


@pytest.mark.parametrize(
    """
    input_file_map,
    output_file_map,
    file_size_limit_in_gb,
    input_scenario,
    file_to_chunk_up,
    expected_chunk_increments
    """,
    [       # input scenario 1: large (ish) input file, small file size limit
        (
            {},
            # Not stipulating any extant output
            {},
            # Setting file size limit for later divvying up
            5 * 10**-6,
            # input scenario
            1,
            "tests/data/get_chunk_increments/all_types_x12.csv",
            # expected chunk increments
            ([1, 94], [93, 120])
        ),

        (   # input scenario 2: large (ish) input file, larger file size limit
            {},
            # Not stipulating any extant output
            {},
            # Setting file size limit for later divvying up
            1,
            # input scenario
            2,
            # file to chunk up
            "tests/data/get_chunk_increments/all_types_x12.csv",
            # expected chunk increments
            ([1], [120])
        ),
        (   # input scenario 3: one line file
            {},
            # Not stipulating any extant output
            {},
            # Setting file size limit for later divvying up
            1,
            # input scenario
            3,
            # file to chunk up,
            "tests/data/get_chunk_increments/all_types_one_line.csv",
            # expected chunk increments
            ([1], [1])
        ),
        (   # input scenario 4: no input files
            {},
            # Not stipulating any extant output
            {},
            # Setting file size limit for later divvying up
            5 * 10**-6,
            # input scenario
            4,
            # not file to chunk up
            None,
            # no expected chunk size
            None
        )
    ]
)
def test_get_chunk_increments(monkeypatch, input_file_map, output_file_map,
                              file_size_limit_in_gb, input_scenario,
                              file_to_chunk_up, expected_chunk_increments):

    pp = setup_packer(monkeypatch, input_file_map, output_file_map,
                      file_size_limit_in_gb)

    # If there's no input, expecting an attribute error
    # when trying to get nrows of df
    if input_scenario == 4:
        with pytest.raises(AttributeError):
            pp._set_file_size_on_disk(None)
    else:
        meta = Metadata.from_json("tests/data/all_types.json")
        file = cast_pandas_table_to_schema(
            reader.read(file_to_chunk_up), meta)

        pp._set_file_size_on_disk(file)
        assert pp._get_chunk_increments() == expected_chunk_increments


@pytest.mark.parametrize(
    """
    input_file_map,
    output_file_map,
    file_size_limit_in_gb,
    input_scenario
    """,
    [       # input scenario 1: plenty of input, no existing output
        (   # dictionary specifying files to copy over to mock land
            {"tests/data/append_files/input_scenario_1/all_types.csv":
             "land/all_types/all_types.csv",
             "tests/data/append_files/input_scenario_1/all_types2.csv":
             "land/all_types/all_types2.csv",
             "tests/data/append_files/input_scenario_1/all_types3.csv":
             "land/all_types/all_types3.csv"
             },
            # Not stipulating any extant output
            {},
            # Setting file size limit for later divvying up
            1,
            # input_scenario
            1
        ),
        (   # input scenario 2: plenty of input, plenty of existing output
            # dictionary specifying files to copy over to mock land
            {"tests/data/append_files/input_scenario_2/all_types.csv":
             "land/all_types/all_types.csv",
             "tests/data/append_files/input_scenario_2/all_types2.csv":
             "land/all_types/all_types2.csv",
             "tests/data/append_files/input_scenario_2/all_types3.csv":
             "land/all_types/all_types3.csv"
             },
            # existing output
            {"tests/data/append_files/input_scenario_2/all_types.snappy.parquet":
             "db/all_types/all_types_0.snappy.parquet",
             "tests/data/append_files/input_scenario_2/all_types_2.snappy.parquet":
             "db/all_types/all_types_1.snappy.parquet",
             "tests/data/append_files/input_scenario_2/all_types_3.snappy.parquet":
             "db/all_types/all_types_2.snappy.parquet",
             "tests/data/append_files/input_scenario_2/all_types_4.snappy.parquet":
             "db/all_types/all_types_4.snappy.parquet"
             },
            # Setting file size limit for later divvying up
            1,
            # input_scenario
            2
        ),
        (   # input sceario 3: no input, plenty of existing output
            # dictionary specifying files to copy over to mock land
            {},
            # existing output
            {"tests/data/append_files/input_scenario_3/all_types.snappy.parquet":
             "db/all_types/all_types_0.snappy.parquet",
             "tests/data/append_files/input_scenario_3/all_types_2.snappy.parquet":
             "db/all_types/all_types_1.snappy.parquet"},
            # Setting file size limit for later divvying up
            1,
            # input_scenario
            3
        ),
        (   # input scenario 4: no input, no existing output
            # dictionary specifying files to copy over to mock land
            {},
            # Not stipulating any extant output
            {},
            # Setting file size limit for later divvying up
            1,
            # input_scenario
            4
        )
    ]
)
def test_get_input_files(monkeypatch, input_file_map, output_file_map,
                         file_size_limit_in_gb, input_scenario):
    # Testing input files by returning and appending them all

    pp = setup_packer(monkeypatch, input_file_map, output_file_map,
                      file_size_limit_in_gb) 
    # If no input files, not expecting to return anything
    # hence expecting a ValueError when we try to concat nothing

    if input_scenario in [3, 4]:
        with pytest.raises(ValueError):
            concat(pp._get_input_files())
    else:
        # only want the results from append_files/input_scenario_1
        # as we don't want to have appended on any existing output
        test_df = (
            reader.read("tests/data/append_files/input_scenario_1"
                        "/appended_output"
                        ".snappy.parquet")
        )
        pp_df = concat(pp._get_input_files())

        assert_frame_equal(test_df, pp_df)


@pytest.mark.parametrize(
    """
    input_file_map,
    output_file_map,
    file_size_limit_in_gb,
    input_scenario,
    expected_latest_file
    """,
    [
        (
            # Input scenario 1: multiple existing outputs, consecutive labelling
            {},
            # output file map
            {
                "tests/data/get_latest_file/input_scenario_1/all_types.snappy.parquet":
                "db/all_types/all_types_0.snappy.parquet",
                "tests/data/get_latest_file/input_scenario_1/all_types_2.snappy.parquet":
                "db/all_types/all_types_2.snappy.parquet"
            },
            1,
            # input scenario
            1,
            # expected latest file
            "tests/data/get_latest_file/input_scenario_1/all_types_2.snappy.parquet"
        ),
        (
            # Input scenario 2: Multiple output files, non-consecutive labelling
            {},
            # output file map
            {
                "tests/data/get_latest_file/input_scenario_2/all_types.snappy.parquet":
                "db/all_types/all_types_0.snappy.parquet",
                "tests/data/get_latest_file/input_scenario_2/all_types_2.snappy.parquet":
                "db/all_types/all_types_2.snappy.parquet",
                "tests/data/get_latest_file/input_scenario_2/all_types_11.snappy.parquet":
                "db/all_types/all_types_11.snappy.parquet"
            },
            1,
            # input scenario
            2,
            # expected latest file
            "tests/data/get_latest_file/input_scenario_2/all_types_11.snappy.parquet"
        ),
        (
            # Scenario 3: no output
            {},
            # Don't need any output files
            {},
            # file size limit
            1,
            # input scenario
            3,
            # expected latest file
            None
        )
    ]
)
def test_get_latest_file(monkeypatch, input_file_map, output_file_map,
                         file_size_limit_in_gb,
                         input_scenario, expected_latest_file):

    pp = setup_packer(monkeypatch, input_file_map, output_file_map,
                      file_size_limit_in_gb)

    # expecting a type error if we try and read in  latest
    # file when there are no existing output files
    if input_scenario == 3:
        with pytest.raises(TypeError):
            pp._get_latest_file()
    else:
        elf = reader.read(expected_latest_file)
        assert_frame_equal(pp._get_latest_file(), elf)


@pytest.mark.parametrize(
    """
    input_file_map,
    output_file_map,
    file_size_limit_in_gb,
    input_scenario,
    file_to_read
    """,
    [
        (
            # testing csv extension
            {},
            {},
            1,
            1,
            "tests/data/all_types.csv"
        ),
        (
            # testing parquet ext
            {},
            {},
            1,
            2,
            "tests/data/all_types.snappy.parquet",
        ),
        (
            # null scenario
            {},
            {},
            1,
            3,
            None
        )
    ]
)
def test_read_file(monkeypatch, input_file_map, output_file_map,
                   file_size_limit_in_gb, input_scenario,
                   file_to_read):

    # Expecting a TypeError if function tries to
    # read a file which doesn't exist
    # Otherwise, testing whether dfs match when casting
    # to a meta data schema and when not casting

    pp = setup_packer(monkeypatch, input_file_map, output_file_map,
                      file_size_limit_in_gb)

    if input_scenario == 3:
        with pytest.raises(TypeError):
            pp._read_file(file_to_read)
    else:
        if input_scenario == 1:
            meta = Metadata.from_json("tests/data/all_types.json")
            test_df = cast_pandas_table_to_schema(
                reader.read(file_to_read),
                meta
            )
            test_pp = pp._read_file(file_to_read)
        else:
            test_df = reader.read(file_to_read)
            test_pp = pp._read_file(file_to_read, ext="snappy.parquet")

        assert_frame_equal(test_pp, test_df)
