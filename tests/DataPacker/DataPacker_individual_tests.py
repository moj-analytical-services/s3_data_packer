from arrow_pd_parser import reader
import pytest
import pandas.testing as pt
from pandas import concat


def test_get_meta(pp, **kwargs):

    if kwargs["inp_s"] > 4:
        assert pp._get_meta("snappy.parquet") is None
    else:
        assert str(type(pp._get_meta("csv"))) == ("<class 'mojap_metadata"
                                                  ".metadata.metadata.Metadata'>")


def test_set_file_size_on_disk(pp, **kwargs):

    fsod = kwargs.get("fsod")
    pp._set_file_size_on_disk(pp._append_files())

    assert round(pp.file_size_on_disk, 9) == fsod


def test_get_chunk_increments(pp, **kwargs):

    pp._set_file_size_on_disk(pp._append_files())

    assert pp._get_chunk_increments() == kwargs["gci"]


def test_get_input_files(pp, **kwargs):
    
    

    assert True


def test_append_files(pp, **kwargs):

    input_scenario = kwargs.get("inp_s")

    appended_files = reader.read((f"tests/data/input_scenario_{input_scenario}"
                                  "/appended_output.snappy.parquet"
                                  ))
    pt.assert_frame_equal(pp._append_files(), appended_files)


def test_get_latest_file(pp, **kwargs):

    latest_file = kwargs.get("lf")
    if latest_file != 0:
        latest_file_test = reader.read(f"tests/data/input_scenario_{latest_file}"
                                       "/all_types.snappy.parquet")

        pt.assert_frame_equal(pp._get_latest_file(), latest_file_test)


def test_read_file(pp, **kwargs):
    assert True


def test_should_append_data(pp, **kwargs):

    assert pp.output_store._should_append_data() == kwargs["sad"]


def test_data_to_add(pp, **kwargs):

    assert pp._data_to_add() == kwargs["dta"]
