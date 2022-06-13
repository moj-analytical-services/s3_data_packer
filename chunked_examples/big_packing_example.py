from s3_data_packer.s3_data_packer import S3DataPacker
import awswrangler as wr

wr.s3.delete_objects("s3://alpha-everyone/s3_data_packer_test/packed")

packer = S3DataPacker(
    "s3://alpha-everyone/s3_data_packer_test/land",
    "s3://alpha-everyone/s3_data_packer_test/packed",
    "big",
    read_chunksize=2500000,
)
packer.pack_data()

packer2 = S3DataPacker(
    "s3://alpha-everyone/s3_data_packer_test/land2",
    "s3://alpha-everyone/s3_data_packer_test/packed",
    "big",
    read_chunksize=2500000,
)
packer2.pack_data()

packer_csv = S3DataPacker(
    "s3://alpha-everyone/s3_data_packer_test/land",
    "s3://alpha-everyone/s3_data_packer_test/packed_csv",
    "big",
    read_chunksize=2500000,
    output_file_ext="csv",
)
packer_csv.pack_data()

packer_csv2 = S3DataPacker(
    "s3://alpha-everyone/s3_data_packer_test/land2",
    "s3://alpha-everyone/s3_data_packer_test/packed_csv",
    "big",
    read_chunksize=2500000,
    output_file_ext="csv",
)
packer_csv2.pack_data()

packer_json = S3DataPacker(
    "s3://alpha-everyone/s3_data_packer_test/land",
    "s3://alpha-everyone/s3_data_packer_test/packed_json",
    "big",
    read_chunksize=2500000,
    output_file_ext="jsonl",
)
packer_json.pack_data()

packer_json2 = S3DataPacker(
    "s3://alpha-everyone/s3_data_packer_test/land2",
    "s3://alpha-everyone/s3_data_packer_test/packed_json",
    "big",
    read_chunksize=2500000,
    output_file_ext="jsonl",
)
packer_json2.pack_data()
