from arrow_pd_parser import reader
import pytest
import pandas.testing as pt
from pandas import concat
import arrow_pd_parser.caster as apc


def test_get_meta(pp, **kwargs):

    if kwargs["inp_s"] > 4:
        assert pp._get_meta("snappy.parquet") is None
    else:
        assert str(type(pp._get_meta("csv"))) == ("<class 'mojap_metadata"
                                                  ".metadata.metadata.Metadata'>")


def test_set_file_size_on_disk(pp, **kwargs):

    if kwargs["inp_s"] == 8:
        with pytest.raises(ValueError):
            pp._set_file_size_on_disk(pp._append_files())
    else:
        fsod = kwargs.get("fsod")
        pp._set_file_size_on_disk(pp._append_files())

        assert round(pp.file_size_on_disk, 9) == fsod


def test_get_chunk_increments(pp, **kwargs):
    if kwargs["inp_s"] == 8:
        with pytest.raises(ValueError):
            pp._set_file_size_on_disk(pp._append_files())
    else:
        pp._set_file_size_on_disk(pp._append_files())

        assert pp._get_chunk_increments() == kwargs["gci"]


def test_get_input_files(pp, **kwargs):
    if kwargs["inp_s"] == 8:
        assert pp._get_input_files() is None
    else:
        meta = kwargs["meta"]
        pp_df = concat(pp._get_input_files())
        test_df = concat([apc.cast_pandas_table_to_schema(reader.read(file), meta)
                         for file in kwargs["ifm"].keys()])

        pt.assert_frame_equal(test_df, pp_df)


def test_append_files(pp, **kwargs):
    if kwargs["inp_s"] == 8:
        with pytest.raises(ValueError):
            appended_files_pp = pp._append_files()
    else:
        appended_files_pp = pp._append_files()

        input_scenario = kwargs.get("inp_s")

        appended_files = reader.read((f"tests/data/input_scenario_{input_scenario}"
                                      "/appended_output.snappy.parquet"
                                      ))
        pt.assert_frame_equal(appended_files_pp, appended_files)


def test_get_latest_file(pp, **kwargs):
    if kwargs["inp_s"] == 8:
        with pytest.raises(TypeError):
            pp._get_latest_file()
    else:
        latest_file = kwargs.get("lf")
        if latest_file != 0:
            latest_file_test = reader.read(f"tests/data/input_scenario_{latest_file}"
                                           "/all_types.snappy.parquet")

            pt.assert_frame_equal(pp._get_latest_file(), latest_file_test)


def test_read_file(pp, **kwargs):
    if kwargs["inp_s"] == 8:
        with pytest.raises(TypeError):
            pp._read_file()
    else:
        df_path = list(kwargs["ifm"].keys())[0]

        if kwargs["inp_s"] > 4:
            test_df = reader.read(df_path)
            pp_df = pp._read_file(df_path, ext="parquet")
            pt.assert_frame_equal(test_df, pp_df)

        else:
            meta = kwargs["meta"]
            test_df = apc.cast_pandas_table_to_schema(reader.read(df_path), meta)
            pp_df = pp._read_file(df_path)
            pt.assert_frame_equal(test_df, pp_df)


def test_should_append_data(pp, **kwargs):
    if kwargs["inp_s"] == 8:
        assert not pp.output_store._should_append_data()
    else:
        assert pp.output_store._should_append_data() == kwargs["sad"]


def test_data_to_add(pp, **kwargs):
    if kwargs["inp_s"] == 8:
        assert not pp._data_to_add()
    else:
        assert pp._data_to_add() == kwargs["dta"]
