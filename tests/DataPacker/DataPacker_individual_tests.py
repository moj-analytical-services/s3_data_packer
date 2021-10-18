from arrow_pd_parser import reader
import pytest
import pandas.testing as pt
from pandas import concat
import arrow_pd_parser.caster as apc


def test_get_files_from_table_log(pp, **kwargs):
    # input_scenario 8 is our fail scenario, so
    # shouldn't be any input files to speak of.
    # testing the full_path option in the function
    # for input_scenarios [1, 4]
    if kwargs["inp_s"] == 8:
        assert pp.input_store.get_files_from_table_log() == []
    else:
        if kwargs["inp_s"] > 4:
            pp_table_log = list(pp.input_store.get_files_from_table_log())
            test_table_log = [ttl.replace("land/all_types/", "")
                              for ttl in list(kwargs["ifm"].values())]
        else:
            pp_table_log = list(pp.input_store.get_files_from_table_log(full_path=True))
            test_table_log = [kwargs["tmp_dir"] + "/" + val
                              for val in list(kwargs["ifm"].values())]

        pp_table_log.sort()
        test_table_log.sort()

        assert pp_table_log == test_table_log


def test_get_table_basepath(pp, **kwargs):
    # the function can't really fail, just verifying that
    # output looks sensible
    assert pp.input_store._get_table_basepath() == (kwargs["tmp_dir"]
                                                    + "/land/all_types/")


def test_get_latest_file_by_suffix(pp, **kwargs):
    # if there is no file in the output file map
    # verify that we don't find a latest file
    # otherwise, ensure latest file has the largest
    # numerical suffix
    if kwargs["ofm"] == {}:
        latest_file = pp.output_store._get_latest_file_by_suffix(
            pp.output_store.get_files_from_table_log())
        assert latest_file is None
    else:
        latest_file_pp = pp.output_store._get_latest_file_by_suffix(
            pp.output_store.get_files_from_table_log())

        latest_file_test = [file.replace("db/all_types/", "")
                            for file in list(kwargs["ofm"].values())]
        latest_file_test.sort()
        latest_file_test = latest_file_test[-1]
        assert latest_file_pp == latest_file_test


def test_get_filenum_from_filename(pp, **kwargs):
    # if there are no files in output_file_map
    # test that function defaults to filenum of 0
    # Otherwise, test that function correctly picks up
    # existing filenum
    if kwargs["inp_s"] == 8:
        assert pp.output_store._get_filenum_from_filename(None) == 0
    else:
        file = pp.output_store._get_latest_file_by_suffix(
            pp.output_store.get_files_from_table_log())
        if kwargs["inp_s"] == 7:
            assert pp.output_store._get_filenum_from_filename(file) == 3
        else:
            assert pp.output_store._get_filenum_from_filename(file) == 0


def test_set_latest_filenum(pp, **kwargs):
    # checks the attribute set using 
    # ._get_filenum_from_filename looks
    # correct
    if kwargs["inp_s"] == 8:
        assert pp.output_store.filenum == 0
    else:
        if kwargs["inp_s"] == 7:
            assert pp.output_store.filenum == 3
        else:
            assert pp.output_store.filenum == 0


def test_get_filename(pp, **kwargs):
    # filename should be tablename_filenum_extension
    if kwargs["inp_s"] == 8:
        assert pp.output_store._get_filename() == "all_types_0.snappy.parquet"
    else:
        if kwargs["inp_s"] == 7:
            assert pp.output_store._get_filename() == "all_types_3.snappy.parquet"
        else:
            pp.output_store._get_filename() == "all_types_0.snappy.parquet"


def test_get_meta(pp, **kwargs):
    # test to see if a metadata object gets returned
    # when extension is not set to parquet/snappy.parquet
    if kwargs["inp_s"] > 4:
        assert pp._get_meta("snappy.parquet") is None
    else:
        assert str(type(pp._get_meta("csv"))) == ("<class 'mojap_metadata"
                                                  ".metadata.metadata.Metadata'>")


def test_set_file_size_on_disk(pp, **kwargs):
    # More of a sanity check that the file size matches
    # when there are no input files to append expecting
    # a ValueError
    if kwargs["inp_s"] == 8:
        with pytest.raises(ValueError):
            pp._set_file_size_on_disk(pp._append_files())
    else:
        fsod = kwargs.get("fsod")
        pp._set_file_size_on_disk(pp._append_files())

        assert round(pp.file_size_on_disk, 9) == fsod


def test_get_chunk_increments(pp, **kwargs):
    # Another sanity check for chunk increments
    # Again, when there are no input files to append
    # expecting a ValueError
    if kwargs["inp_s"] == 8:
        with pytest.raises(ValueError):
            pp._set_file_size_on_disk(pp._append_files())
    else:
        pp._set_file_size_on_disk(pp._append_files())

        assert pp._get_chunk_increments() == kwargs["gci"]


def test_get_input_files(pp, **kwargs):
    # Testing input files by returning and appending them all
    # If no input files, not expecting to return anything
    if kwargs["inp_s"] == 8:
        assert pp._get_input_files() is None
    else:
        meta = kwargs["meta"]
        pp_df = concat(pp._get_input_files())
        test_df = concat([apc.cast_pandas_table_to_schema(reader.read(file), meta)
                         for file in kwargs["ifm"].keys()])

        pt.assert_frame_equal(test_df, pp_df)


def test_append_files(pp, **kwargs):
    # Again, if there are no files to append
    # expecting a ValueError
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
    # Expecting a TypeError if we try and
    # read in a file which doesn't exist,
    # given no outputs exist
    # Otherwise, if there are current output files,
    # test they match expected
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
    # Expecting a TypeError if function tries to
    # read a file which doesn't exist
    # Otherwise, testing whether dfs match when casting
    # to a meta data schema and when not casting
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
    # default is false, can't fail past false
    # test to see whether new data should be appended with
    # existing outputs
    if kwargs["inp_s"] == 8:
        assert not pp.output_store._should_append_data()
    else:
        assert pp.output_store._should_append_data() == kwargs["sad"]


def test_data_to_add(pp, **kwargs):
    # again, default is false so can't fail past it
    # test to see whether there's new data to pack
    if kwargs["inp_s"] == 8:
        assert not pp._data_to_add()
    else:
        assert pp._data_to_add() == kwargs["dta"]
