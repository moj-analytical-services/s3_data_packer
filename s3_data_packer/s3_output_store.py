import os
import boto3

from typing import List, Union
from dataengineeringutils3.s3 import s3_path_to_bucket_key
from s3_data_packer.s3_table_store import S3TableStore
from s3_data_packer.constants import default_file_limit_gigabytes


def _get_s3_file_head(f: str):
    s3_client = boto3.client("s3")
    bk, ky = s3_path_to_bucket_key(f)
    return s3_client.head_object(Bucket=bk, Key=ky)


class S3OutputStore(S3TableStore):
    def __init__(
        self,
        basepath: str,
        table_suffix: str = None,
        file_limit_gigabytes: int=default_file_limit_gigabytes,
        **kwargs,
    ):

        super().__init__(basepath, **kwargs)
        if not self.table_extension:
            self.table_extension = "snappy.parquet"
        self.file_limit_gigabytes = file_limit_gigabytes
        self.table_suffix = table_suffix

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
        return file

    def _get_filenum_from_filename(self, f: Union[str, None]) -> int:
        return int(os.path.splitext(os.path.basename(f))[0].split(".")[0].split("_")[-1]) if f else 0

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
            resp = _get_s3_file_head(self.latest_file)

            # step 3: if over the limit, lets start a new file! (yay!)
            file_lim_bytes = self.file_limit_gigabytes * (10 ** 9)
            if resp.get("ContentLength", 0) >= file_lim_bytes and True:
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
