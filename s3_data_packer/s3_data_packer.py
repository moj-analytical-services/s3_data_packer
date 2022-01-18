# import arrow_pd_parser

import os
import tempfile

import numpy as np

from arrow_pd_parser import reader, writer
from arrow_pd_parser.caster import cast_pandas_table_to_schema
from mojap_metadata.metadata.metadata import Metadata
from pandas import DataFrame, concat
from s3_data_packer.constants import default_file_limit_gigabytes
from s3_data_packer.helpers import get_file_format
from s3_data_packer.s3_output_store import S3OutputStore
from s3_data_packer.s3_table_store import S3TableStore
from typing import List, Tuple, Union


class S3DataPacker:
    def __init__(
        self,
        input_basepath: str,
        output_basepath: str,
        table_name: str,
        metadata: Union[str, dict, Metadata] = None,
        output_file_ext: str = "snappy.parquet",
        output_suffix: str = None,
        input_file_ext: str = None,
        cast_parquet: bool = False,
        output_partition: dict = None,
        input_partition_name: str = None,
        file_limit_gigabytes: int = default_file_limit_gigabytes,
    ):

        # set the blank table_name property. Has to be the _ one as it depends on itself
        self._table_name = None

        self.cast_parquet = cast_parquet

        # build the input store
        self.input_store = S3TableStore(
            input_basepath, table_name, partition_name=input_partition_name
        )
        if input_file_ext is None:
            self.input_store.table_extension = get_file_format(
                self.input_store._get_table_basepath()
            )
        else:
            self.input_store.table_extension = input_file_ext

        # build the output store
        self.output_store = S3OutputStore(
            output_basepath,
            table_name=table_name,
            table_suffix=output_suffix,
            partition=output_partition,
            file_limit_gigabytes=file_limit_gigabytes,
        )
        found_output_file_ext = get_file_format(self.output_store._get_table_basepath())

        # if the output source contains files already, they must be of the same format
        if found_output_file_ext and output_file_ext:
            if not found_output_file_ext.endswith(output_file_ext):
                raise TypeError(
                    "output table path cotains files with different file extensions. "
                    f"found: '{found_output_file_ext}' specified: '{output_file_ext}'"
                )

        self.output_store.table_extension = output_file_ext
        # reset to rediscover files of the given extension
        self.output_store._reset()

        # set it not blank now
        self.table_name = table_name

        # read the metadata
        self.metadata = Metadata.from_infer(metadata) if metadata else metadata

        if self.metadata is not None:
            self.metadata.set_col_type_category_from_types()

    def _get_meta(self, ext: str = None) -> Union[Metadata, None]:
        meta = (
            self.metadata
            if self.cast_parquet or ext not in ["snappy.parquet", "parquet"]
            else None
        )
        return meta

    def _set_file_size_on_disk(self, df: DataFrame):
        # write the data and get it's size on disk
        with tempfile.NamedTemporaryFile(
            suffix="." + self.output_store.table_extension
        ) as t:
            self.table_nrows = df.shape[0]
            writer.write(df, t.name)
            self.file_size_on_disk = os.path.getsize(t.name) / (10 ** 9)

    def _get_chunk_increments(self) -> Tuple[List[int], List[int]]:
        increment_size = max(
            int(
                np.floor(
                    (self.output_store.file_limit_gigabytes / self.file_size_on_disk)
                    * self.table_nrows
                )
            ),
            1,
        )
        increment_start = np.arange(
            0, max(self.table_nrows, 1), increment_size
        ).tolist()
        increment_end = np.arange(
            increment_size, self.table_nrows, increment_size
        ).tolist()
        increment_end.append(self.table_nrows)

        return (increment_start, increment_end)

    def _get_input_files(self) -> List[DataFrame]:
        # get a list of input files as Dataframes
        raw_tables = [
            self._read_file(fp, self.input_store.table_extension)
            for fp in self.input_store.get_files_from_table_log(full_path=True)
        ]
        return raw_tables

    def _append_files(self) -> DataFrame:
        # concat all new and the most recent file
        raw_tables = self._get_input_files()
        if self.output_store._should_append_data():
            existing_df = self._get_latest_file()
            total_df = concat([existing_df, *raw_tables])
        else:
            total_df = concat(raw_tables)

        return total_df

    def _get_latest_file(self) -> DataFrame:
        # read the latest file into a pandas df
        path = self.output_store._get_latest_file_by_suffix(
            self.output_store.get_files_from_table_log(), full_path=True
        )
        df = self._read_file(path, self.output_store.table_extension)
        return df

    def _read_file(self, fp: str, ext: str = None) -> DataFrame:
        meta = self._get_meta(ext)
        df = reader.read(fp)
        df = cast_pandas_table_to_schema(df, meta) if meta else df
        return df

    def pack_data(self):
        # any data to even add?
        if not self.input_store.get_files_from_table_log():
            return
        # collate all the data from s3
        total_df = self._append_files()
        # get the size on disk
        self._set_file_size_on_disk(total_df)
        # get the indexes to write to
        start_vals, end_vals = self._get_chunk_increments()
        for i in range(len(start_vals)):
            # break up the df
            df = total_df[start_vals[i] : end_vals[i]]
            out_path = self.output_store._get_filename(full_path=True)
            # output the pandas df
            writer.write(df, out_path, metadata=self.metadata)
            # increment the filenumber
            self.output_store.filenum += 1

    @property
    def table_name(self):
        return self._table_name

    @table_name.setter
    def table_name(self, new_table_name: str):
        if new_table_name != self._table_name and self._table_name is not None:
            self.input_store.table_name = new_table_name
            self.output_store.table_name = new_table_name
        self._table_name = new_table_name

    @property
    def output_basepath(self):
        return self.output_store.output_basepath

    @output_basepath.setter
    def output_basepath(self, new_output_basepath: str):
        if new_output_basepath != self.output_store.basepath:
            self.output_store.basepath = new_output_basepath

    @property
    def input_basepath(self):
        return self.input_store.basepath

    @input_basepath.setter
    def input_basepath(self, new_input_basepath: str):
        if new_input_basepath != self.input_store.basepath:
            self.input_store.basepath = new_input_basepath

    @property
    def cast_parquet(self):
        return self._cast_parquet

    @cast_parquet.setter
    def cast_parquet(self, new_cast_parquet: bool):
        self._cast_parquet = new_cast_parquet
