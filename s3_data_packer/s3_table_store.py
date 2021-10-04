import os

from dataengineeringutils3 import s3


class S3TableStore:
    def __init__(
        self,
        basepath: str,
        table_name: str = None,
        table_extension: str = None,
    ):

        # set all properties to None
        self._table_name = None
        self._basepath = None

        self._attrs_needed_for_reset = ["table_name", "basepath"]
        self.table_extension = table_extension
        self.basepath = s3._add_slash(basepath)
        self.table_name = table_name
        self._init_table_log()

    def _init_table_log(self):
        """Gets the file log from s3 if it exists"""

        self.table_log = {}

        files = s3.get_filepaths_from_s3_folder(
            self.basepath, file_extension=self.table_extension
        )
        for f in files:
            table_name, filename = f.replace(self.basepath, "", 1).split("/", 1)
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
            files = [os.path.join(self._get_table_basepath(), f) for f in files]
        return files

    def _reset(self):
        reset = True
        for attr_name in self._attrs_needed_for_reset:
            attr = getattr(self, attr_name)
            if attr is None:
                reset = False
        if reset:
            self._init_table_log()

    @property
    def table_name(self):
        return self._table_name

    @table_name.setter
    def table_name(self, new_table_name: str):
        self._table_name = new_table_name
        if new_table_name is not None:
            self._reset()

    @property
    def basepath(self):
        return self._basepath

    @basepath.setter
    def basepath(self, new_basepath: str):
        self._basepath = new_basepath
        if new_basepath is not None:
            self._reset()
