# s3_data_packer

## what is s3_data_packer?
s3_data_packer contains 3 classes used for the management of files in s3 for uniform 
distribution and collation of files in an easy API.
It contains 3 useful classes:

[S3TableStore](#S3TableStore):
an easy object for listing files in an s3 folder structure of `basepath/table_name`. 
When basepath or table_name are created. When either `basepath` or `table_name` are 
updated, the table log is also updated

[S3OutputStore](#S3OutputStore):
used for determining a regular formatting output files in the either format:
- `s3://basepath/table_name/table_name_0.extension`
- `s3://basepath/table_name/table_name_suffix_0.extension`
it also has a number of useful methods around determining what is the latest file and 
if the file is under a given file limit

[S3DataPacker](#S3DataPacker):
packs data of any input format to any output format in a regular format (as prescribed 
by `S3OutputStore`) to a given size limit

## Simple Usage

### Simple Packing csv Files to snappy.parquet

Lets say in a bucket called `s3://some-bucket/land/table/` you have the following files:
```
some-bucket/
├─ land/
│  ├─ table/
│  │  ├─ table_data_final_NEW_1969.csv   1gb
│  │  ├─ table_data_new.csv              512mb
```
and you wanted to distibute these files evenly in `s3:///some-bucket/db/table/` with no 
existing output files:
```python
from s3_data_packer import S3DataPacker
packer = S3DataPacker(
    "s3://some-bucket/land",
    "s3://some-bucket/db",
    "table"
)
packer.pack_data()
```

would give you the following output in `s3://some-bucket/db/table`:
```
some-bucket/
├─ db/
│  ├─ table/
|  │  ├─ table_0.snappy.parquet    256mb
|  │  ├─ table_1.snappy.parquet    256mb
|  │  ├─ table_2.snappy.parquet    256mb
|  |  ├─ table_3.snappy.parquet    256mb
|  │  ├─ table_4.snappy.parquet    256mb
|  │  ├─ table_5.snappy.parquet    256mb
```

if there were some existing data, this existing data would be incoorprated into the new 
packed data, and that file would be filled up until it was at the file limit, and the 
rest of the files are made as shown above. 

_what if the numbers aren't as easy as shown above?_

The numbers shown are very round, so lets say under tha same situatuion, we have the 
following as an input:
```
some-bucket/
├─ land/
│  ├─ table/
│  │  ├─ table_data_final_NEW_1969.csv   1gb
│  │  ├─ table_data_new.csv              512mb
```

and the following already exists in the output location:
```
some-bucket/
├─ db/
│  ├─ table/
|  │  ├─ table_0.snappy.parquet    128mb
```

with the same script shown for the first example, it would produce the following output:
```
some-bucket/
├─ db/
│  ├─ table/
|  │  ├─ table_0.snappy.parquet    256mb
|  │  ├─ table_1.snappy.parquet    256mb
|  │  ├─ table_2.snappy.parquet    256mb
|  |  ├─ table_3.snappy.parquet    256mb
|  │  ├─ table_4.snappy.parquet    256mb
|  │  ├─ table_5.snappy.parquet    256mb
|  │  ├─ table_6.snappy.parquet    128mb
```
Files are filled up in order. The data from the original `table_0.snappy.parquet` is 
still in that file, it has now just been concatenated with other data.

`S3DataPacker` is format agnostic, although it defaults to outputting as 
`snappy.parquet`, it can be used to chunk csv to jsonl, parquet to csv, or even csv to csv, basically any(jsonl, csv, parquet) to any(jsonl, csv, parquet) under the 
same sequential filling of data shown

### Partitions and packing data

if you have a database with partitions, you may want to pack the data evenly in a given partition. So take the following input data:
```
some-bucket/
├─ land/
│  ├─ table/
│  │  ├─ table_data_final_NEW_1969.csv   1gb
│  │  ├─ table_data_new.csv              512mb
```

and the following data alreay in the output:
```
some-bucket/
├─ db/
│  ├─ table/
|  │  ├─ some_value=1649-01-30
|  |  │  ├─ table_0.snappy.parquet    128mb
```

if you want to add the data to this partition, you would:
```python
from s3_data_packer import S3DataPacker
packer = S3DataPacker(
    "s3://some-bucket/land",
    "s3://some-bucket/db",
    "table"
    output_partition = {"some_value": "1649-01-30"}
)
packer.pack_data()
```

if you wanted to add data to a new partition, simply change the value given in the `output_partition` dictionary kwarg

### Why?

Primarily, this is used to create Athena database parquet files that are evenly 
distributed to ensure that databases can be queried efficiently and avoiding skew.

## Detailed interface

## S3TableStore
This is the base to `S3OutputStore`. 

_properties_
set on initialisation arguments:
- `basepath`: str (required), an s3 formatted string (beginning "s3://"). This is the 
path that contains folder(s) that contain data. The folders in the basepath **must** be 
named after the table. can trigger `_reset`
- `table_name`: str (optional) = None, the name of the table in question. This optional 
on initialisation, but should be set before calling any ofthe public methods. can 
trigger `_reset`
- `table_extension`: str (optional) = None, file format of the files. If set, files are 
filtered on this else all files are considered
(supported: `csv`, `jsonl`, `parquet`, `snappy.parquet`)
- `parition_name`: str (optional) = None, the name of the partition used in this data set. It is used to populate
`S3TableStore.partition_values`

not set on intialisation arguments:
- `table_log`: dict{str: list}, a dictionary in the format:
```python
{
    "table_name": ["table_file_0.csv", "table_file_1.csv" ... "table_file_n.csv"]
}
```
This is created by `_init_table_log` to give a list of all files in the table path.

- `_attrs_needed_for_reset`: list[str], a list of properties that need to be set for
`_reset` to trigger

While `table_name` is optional on initialisation, it should be set before calling any 
public methods.

_public methods_
- `get_files_from_table_log`(`full_path`: bool = `False`) -> list:
args:
    - `full_path`: set to `False` by default. If true, it will return the full path of 
    the latest file includin the "s3://" prefix 
returns:
    - Returns a lits of files from the table log (see `_init_table_log`)
    this will return a list of files from the table log for the give n `table_name`.

_private methods_
- `_init_table_log`(None): -> None 
queries s3 for the files in `basepath/table_name/*` and sets the `table_log` property. 
takes no arguements. returns nothing.

- `_get_table_basepath`(None): -> str
takes no arguemnts.
returns:
    - returns the joing of `basepath` and `table_name` with a trailing slash

- `_reset`: this is used interaly to call `_init_table_log` if `basepath` or 
`table_name` changes. It only calls `_init_table_log` if both have been set. takes no 
arguments. returns nothing.


## S3OutputStore(S3TableStore)
_properties_
set on initialisation arguments:
- `file_limit_gigabytes`: int | float (optional), this is the limit for files to 
be considered "too big" and that must not be appended to anymore. In gigabytes.
- `table_suffix`: str (optional) =  None, a suffix for the table, this 
suffix goes before the filenumber but after the filename, making the output: 
`{table_name}_{table_suffix}_{file_num}.{table_extension}`
- `partition`: dict (optional) = None, a dictionary mapping of the partition/partition 
value you want to write the data too. S3OutputStore will only consider data in this
partition for it's calculations (e.g. latest file)

`table_extension` defaults to `snappy.parquet` for `S3OutputStore`

not set on initialisation arguments:
- `filenum`: int, as the output of `_get_latest_filenum` on call of 
`_set_latest_filenum` in `_reset`
- `latest_file`

_private methods_
- `_get_latest_file_by_suffix`(`files`: list, `maxim`: bool = True, `full_path`: bool = False): -> str
args:
    - `files`: A list of files to parse and extract the latest file from. This relies 
    on the file list containing files in the format `file_name_{file_num}.ext`, which 
    is the format `S3DataPacker` enforces. It is called internally with the output of 
    `get_files_from_table_log`
    - `maxim`: if true, the maximum file number suffix is considered the latest, if 
    `False`, then the lowest number is considered the latest file.
    - `full_path`: whether or not to return just the file name or the full path 
    including the "s3://" prefix
returns:
    - the latest file from the file log, using the above logic.

- `_get_filenum_from_filename`(`f`: str) -> int
args:
    - `f`: filename to extract the filenumber from. it expexts the filenumber to be the 
    last element of an underscore seperated filename prior to any periods in. E.g. 
    `some_data_file_suffix_10.snappy.parquet`.
returns:
    - the number as described above as an interger

- `_set_latest_filenum`(None) -> None
Used internally to set what the latest (by defualt, by largest filnum suffix). If no 
files present in the table basepath, then it sets to 0

- `_should_append_data`(None) -> bool
returns:
    determines whether the latest file (from the output of `_get_latest_file_by_suffix`) 
    should be appended to based on the `file_limit_gigabytes` (default 256mb).
    returns:

- `_get_filename`(`full_path`: bool = `False`) -> str
args:
    - `full_path`: set to `False` by default, if `True` will return the full path 
    inclding "s3://" prefix
returns:
    - the expected next filename out, in the format 
    `{table_name}_{table_suffix}_{file_num}.{table_extension}` or 
    `{table_name}_{file_num}.{table_extension}` if no suffix is specified

- `_reset`: used to call `_init_table_log` and `_set_latest_filenum` with the same logic
 as in `S3TableStore` takes no arguments. returns nothing.

## S3DataPacker

_properties_

set on initialisation arguments
- `input_basepath`: str (required), the location of the table(s) input file(s) used as 
the input. The files **must** be in a folder with the same name as `table_name`. 
passed as `basepath` to an object of `S3TableStore`
- `output_baspath`: str (required), the location of the table(s) output file(s) where 
all outputs are written to, and any pre-existing outputs are. The files will be placed 
in folder with the same name as `table_name`. If files are to be appended, they **must**:
    - stick to the naming convention of `{file_name_usually_table_name}_{table_suffix_if_applicable}_{filenum}.{ext}`
    - be in a folder with the same name as `table_name`
passed as `basepath` to an object of `S3OutputStore`.
- `table_name`: str (required), the name of the table being created or appended to.
passed as `table_name` to an object of each of `S3TableStore` and `S3OutputStore`
- `metadata`: dict | Metadata | str (optional) = None, metadata of the selected table. 
Defaults to None.
- `output_file_ext`: str (optional) = `snappy.parquet`, the file extension of the 
output required. If there are existing files in the output path 
(`{output_basepath/table_name/*}`) that are of a different file format, an error is 
raised. passed as `table_extension` to and object of `S3OutputStore`
- `input_file_ext`: str (optional) = None, the file extension filter for input files 
(supported: `csv`, `jsonl`, `parquet`, `snappy.parquet`). passed as `table_extension` 
to an object of `S3TableStore`
- `cast_parquet`: bool (optional) = False, cast parquet to metadata or not. Casting 
parquet files can cause issues in some edge cases
- `output_suffix`: str (optional) = None, see `table_suffix` in `S3OutputStore` above. 
passed as `table_suffix` to an object of `S3OutputStore`
- `file_limit_gigabytes`: int | float (optional) = 256*10^-3, this is the limit 
for files to be considered "too big" and that must not be appended to anymore. In 
gigabytes. passed as `file_limit_gigabytes` to an object of `S3OutputStore`
- `output_partition`: dict (optional) = None, passed to `S3OutputStore` as `partition`.
data will be written to this partition (appened or otherwise)
- `input_partition_name`: str (optional) = None, passed to `S3TableStore` as
`partition_name`. Has no real effect in this instance, but is used to populate
`S3DataPacker.input_store.partition_values` if reading in from a partitioned source
- `read_chunksize`: int or str (optional) = None. If not None this is used by 
`arrow_pd_parser.reader` to read data in chunks, which is useful when the data is
too large to fit into memory.

not set from initialisation arguments
- `input_store`: S3TableStore, initilaised with `input_basepath`, `table_name`, and 
`input_file_ext`
- `output_store`: S3OutputStore, initialised with `output_basepath`, `table_name`, 
`output_file_ext`, and `output_suffix`
- `file_size_on_disk`

_public methods_
- `pack_data`(None): -> None
distributes all files from the output of `_append_files` to 
`output_basepath/table_name` in file sizes approx `file_limit_gigabytes` in the format 
described in `S3OutputStore._get_filename`

_private methods_
- `_get_meta`(ext: str = None): -> Metadata | None, 
returns 
metadata object if:
    - it has been set by metadata property AND
    - if output file is not `parquet` or `snappy.parquet` and if it is:
    - `cast_parquet` is `True`
else, None
- `_set_file_size_on_disk`(df: DataFrame) -> None
sets `file_size_on_disk` of the all the data that needs to be chunked to s3
- `_get_chunk_increments`(None): -> tuple(list[int], list[int])
returns:
    - two lists of intergers, where the first list is the start indexes of the 
    dataframe and the second are the end indexes of the dataframe required to meet the 
    `file_limit_gigabytes`
- `_get_input_files`(None): -> list[DataFrame]
returns:
    - a list DataFrames that are to be added to the output
- `_append_files`(): -> DataFrame
returns:
    - the full DataFrame of all data that is to be chunked and written to s3 inlcuding 
    any existing data if the output of `output_store._should_append_data()` is `True`
- `_get_latest_file`(None): -> DataFrame
returns:
    - gets the existing latest file as a DataFrame from 
    `output_store._get_latest_file_by_suffix`
- `_read_file`(fp: str, ext: str = None) -> DataFrame
args:
    - `fp`: the full "s3://" prefixed file path to be read
    - `ext`: the extension of the file being read, passsed to `_get_meta`
returns:
    - a DataFrame cast to meta if `_get_meta` returns Metadata

- `_data_to_add`(None): -> bool
returns:
    - whether there is any data to add to the output table or not by checking whether 
    there are any files in `input_store.table_log`
