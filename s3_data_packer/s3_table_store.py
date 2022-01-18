import os
import re

from dataengineeringutils3 import s3
from s3_data_packer.helpers import _list_files_in_path


class S3TableStore:

    _attrs_needed_for_reset = ["table_name", "basepath"]

    def __init__(
        self,
        basepath: str,
        table_name: str = None,
        table_extension: str = None,
        partition_name: str = None,
    ):

        # set all properties to None
        self._table_name = None
        self._basepath = None

        # self._attrs_needed_for_reset = ["table_name", "basepath"]
        self.partition_name = partition_name
        self.table_extension = table_extension
        self.basepath = s3._add_slash(basepath)
        self.table_name = table_name
        self._init_table_log()

    def _init_table_log(self):
        """Gets the file log from s3 if it exists"""

        self.table_log = {}

        files = _list_files_in_path(self.basepath, self.table_extension)
        for f in files:
            try:
                table_name, filename = f.replace(self.basepath, "", 1).split("/", 1)
            except ValueError:
                # if the tablename can't be extracted from the path
                continue

            # if partition is present, remove the partition followed by the value
            # assign the partition values to a new instance variable
            if self.partition_name is not None:
                reg_expr = re.compile(f".+{self.partition_name}=(.+)/(.+)$")
                reg_search = reg_expr.search(f)
                if reg_search:
                    filename = reg_search.groups()[1]
                    self.partition_values.append(reg_search.groups()[0])

            if table_name not in self.table_log:
                self.table_log[table_name] = []
            self.table_log[table_name].append(filename)

    def _get_table_basepath(self):
        """joins the basepath and the table name together"""
        out = os.path.join(
            self.basepath,
            self.table_name,
        )
        return s3._add_slash(out)

    def get_files_from_table_log(self, full_path: bool = False):
        """returns files for the given table from the file log"""
        files = self.table_log.get(self.table_name, [])
        if full_path:
            if self.partition_name is not None:
                files = _list_files_in_path(self._get_table_basepath())
            else:
                files = [os.path.join(self._get_table_basepath(), f) for f in files]
        return files

    def _reset(self):
        reset = True
        for attr_name in self._attrs_needed_for_reset:
            attr = getattr(self, attr_name)
            if attr is None:
                reset = False
                break
        if reset:
            self._init_table_log()

    @property
    def table_name(self):
        return self._table_name

    @table_name.setter
    def table_name(self, new_table_name: str):
        self._table_name = new_table_name
        self._reset()

    @property
    def basepath(self):
        return self._basepath

    @basepath.setter
    def basepath(self, new_basepath: str):
        self._basepath = new_basepath
        self._reset()

    @property
    def partition_name(self):
        return self._partition_name

    @partition_name.setter
    def partition_name(self, new_partition_name):
        self._partition_name = new_partition_name
        if new_partition_name is not None:
            self.partition_values = []
        self._reset()
