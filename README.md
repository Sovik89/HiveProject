**HiveProject
Hive Project to convert csv files for NYSE data to parquet file with partition key as trademonth(YYYYmm)

source data-csv scenario 1: from local scenario 2:from hdfs

target:parquet file partition key as trademonth(YYYYmm)

triggered by shell wrapper(nyse_loader.sh)

Also find in the document how it was scheduled as cron job

options: */3 * * * * command >> /var/log/log_file_path.log
