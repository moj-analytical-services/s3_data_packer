import os
import re

from typing import List, Union
from s3_data_packer.helpers import _get_file_size
from s3_data_packer.s3_table_store import S3TableStore
from s3_data_packer.constants import default_file_limit_gigabytes


class S3OutputStore(S3TableStore):
    def __init__(
        self,
        basepath: str,
        table_name: str = None,
        table_suffix: str = None,
        partition: dict = None,
        file_limit_gigabytes: int = default_file_limit_gigabytes,
        **kwargs,
    ):

        # set these to stop attribute errors
        self._partition = None
        self._partition_name = None

        super().__init__(basepath, table_name, **kwargs)

        self.partition = partition

        self.file_limit_gigabytes = file_limit_gigabytes
        self.table_suffix = table_suffix

        if not self.table_extension:
            self.table_extension = "snappy.parquet"

    def get_files_from_table_log(self, full_path: bool = False) -> list:
        if self.partition is not None:
            reg_expr = re.compile(
                f"^.+{self.partition_name}={self.partition_values[0]}/.+"
            )
            files = super().get_files_from_table_log(full_path=True)
            files = [f for f in files if reg_expr.search(f)]
            if not full_path:
                files = [os.path.basename(f) for f in files]
        else:
            files = super().get_files_from_table_log(full_path=full_path)

        return files

    def _get_latest_file_by_suffix(
        self, files: List[str], maxim: bool = True, full_path: bool = False
    ) -> str:
        i = -1 if maxim else 0
        sf = sorted(files, key=self._get_filenum_from_filename)
        if files and sf:
            file = sf[i]
            if full_path:
                file = os.path.join(self._get_table_basepath(), file)
        else:
            file = None

        # get latest file in only the specified partition
        if self.partition is not None and file is not None:
            reg_expr = re.compile(
                f"^.+{self.partition_name}={self.partition_values[0]}/.+"
            )
            if not reg_expr.search(file):
                file = None

        return file

    def _get_filenum_from_filename(self, f: Union[str, None]) -> int:
        f = "" if not f else f
        if re.search("_[0-9]+.", f):
            return int(
                os.path.splitext(os.path.basename(f))[0].split(".")[0].split("_")[-1]
            )
        else:
            raise ValueError(f"filename not in data_packer format: {f}")

    def _set_latest_filenum(self):
        if self.latest_file:
            self.filenum = self._get_filenum_from_filename(self.latest_file)
        else:
            self.filenum = 0

    def _should_append_data(self) -> bool:
        # the next writable file is the next most recent file that's under the size
        # step 2: get the detail about the file, if there is one
        append = False
        if self.latest_file:
            file_size_bytes = _get_file_size(self.latest_file)

            # step 3: if over the limit, lets start a new file! (yay!)
            file_lim_bytes = self.file_limit_gigabytes * (10 ** 9)
            if file_size_bytes >= file_lim_bytes and True:
                self.filenum += 1
            else:
                append = True

        return append

    def _get_filename(self, full_path=False):
        if self.table_suffix:
            ret_pth = (
                f"{self.table_name}_{self.table_suffix}_{self.filenum}"
                f".{self.table_extension}"
            )
        else:
            ret_pth = f"{self.table_name}_{self.filenum}.{self.table_extension}"
        if full_path:
            if self.partition is not None:
                ret_pth = os.path.join(
                    f"{self.partition_name}={self.partition_values[0]}", ret_pth
                )
            ret_pth = os.path.join(self._get_table_basepath(), ret_pth)
        return ret_pth

    def _reset(self):
        reset = True
        for attr_name in self._attrs_needed_for_reset:
            attr = getattr(self, attr_name)
            if attr is None:
                reset = False
        if reset:
            self._init_table_log()
            file_list = self.get_files_from_table_log(full_path=True)
            self.latest_file = self._get_latest_file_by_suffix(file_list)
            self._set_latest_filenum()

    @property
    def partition(self):
        return self._partition

    @partition.setter
    def partition(self, new_partition: dict):
        """
        sets the partition name and value from a dictionary in the format:
            {"partition_name": "partition_value"}
        """
        self._partition = new_partition
        if new_partition is not None:
            self._partition_name = list(new_partition.keys())[0]
            self.partition_values = [new_partition[self.partition_name]]
            self._reset()
        else:
            self.partition_values = []
