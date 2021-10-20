from arrow_pd_parser import reader
import pytest
from pandas.testing import assert_frame_equal
from pandas import concat
from arrow_pd_parser.caster import cast_pandas_table_to_schema
from helpers import setup_packer


@pytest.mark.parametrize(
    """
    input_file_map,
    output_file_map,
    file_size_limit_in_gb
    """,
    [
        (   # dictionary specifying files to copy over to mock land
            {"tests/data/append_files/all_types.csv":
             "land/all_types/all_types.csv",
             "tests/data/append_files/all_types2.csv":
             "land/all_types/all_types2.csv",
             "tests/data/append_files/all_types3.csv":
             "land/all_types/all_types3.csv"
             },
            # Not stipulating any extant output
            {},
            # Setting file size limit for later divvying up
            1
        )
    ]
)
def test_append_files(monkeypatch, input_file_map,
                      output_file_map, file_size_limit_in_gb):

    pp = setup_packer(monkeypatch, input_file_map, output_file_map,
                      file_size_limit_in_gb)

    appended_files_pp = pp._append_files()
    appended_files_expected = reader.read("tests/data/append_files/"
                                          "appended_output.snappy.parquet")
    assert_frame_equal(appended_files_pp, appended_files_expected)
