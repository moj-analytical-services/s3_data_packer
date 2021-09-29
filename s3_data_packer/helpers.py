import os

from collections import Counter
from dataengineeringutils3 import s3

def get_file_format(pth: str): # pth has "s3://"" infront
    # are there any files for us to infer from?
    files_in_path = s3.get_filepaths_from_s3_folder(pth)
    if not len(files_in_path):
        return None
    
    # there are files to infer from, but do they all have the same ff?
    ff_list = [f.split(".", 1)[-1] for f in files_in_path]
    found_ffs = list(Counter(ff_list).keys())
    if len(found_ffs) != 1:
        raise ValueError(
            "found more than one file format in table path"
            f"in {pth}: {', '.join(found_ffs)}"
        )
    
    return found_ffs[0]
