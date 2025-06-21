from pyspark.sql.types import StructType
from pyspark.sql import SparkSession
import gc
import pandas as pd

class HudiStorage:
    
    @staticmethod
    def write_to_hudi(updated_records_data, rulebase_schema, primary_key, glue_db, hudi_table_name, partition_path, precombined_field, bucket_name, s3_path, spark, hudistorage_flag_path):
        try:
            print("Writing to Hudi table...")

            filename = f's3://{bucket_name}{s3_path}{hudi_table_name}/'

            # Create DataFrame from dict and schema
            df = spark.sparkContext.parallelize(updated_records_data).toDF(schema=eval(rulebase_schema))
            print("Spark DataFrame row count:", df.count())
            df.show()

            hudi_options = {
                "hoodie.table.name": hudi_table_name,
                "hoodie.database.name": glue_db,
                "hoodie.datasource.write.recordkey.field": primary_key,
                "hoodie.datasource.write.precombine.field": precombined_field,
                "hoodie.datasource.write.operation": "insert",
                "hoodie.insert.shuffle.parallelism": 300,
                "hoodie.write.concurrency.mode": "SINGLE_WRITER",
                "hoodie.datasource.hive_sync.enable": "true",
                "hoodie.datasource.hive_sync.database": glue_db,
                "hoodie.datasource.write.row.writer.enable": "false",
                "spark.sql.parquet.compression.codec": "snappy",
                "hoodie.index.type": "RECORD_INDEX",
                "hoodie.metadata.record.index.enable": "false",
                "hoodie.execution.insert.size_based_split.enable": "true",
                "hoodie.insert.size.target.file.size": 256 * 1024 * 1024,
                "hoodie.parquet.small.file.limit": 128 * 1024 * 1024,
                "hoodie.parquet.max.filesize": 256 * 1024 * 1024
            }

            # Write data to Hudi
            df.write.format("org.apache.hudi")\
                .options(**hudi_options)\
                .mode("append")\
                .save(filename)

            df.unpersist()
            gc.collect()
            return True

        except Exception as e:
            print(f"Hudi write failed: {e}")
            if 'DATA_SOURCE_NOT_FOUND' in str(e) or 'ERROR AsyncEventQueue' in str(e):
                print("Handled known transient error.")
                pass
            else:
                with open(hudistorage_flag_path, 'a') as flag_file:
                    flag_file.write("\nHUDI_DATA_INSERTION_ERROR")
                return False

    @staticmethod
    def read_hudi_for_notifications(hudi_table_name, spark, hudi_bucket_name, hudi_s3_path):
        filename = f's3://{hudi_bucket_name}{hudi_s3_path}{hudi_table_name}/'

        try:
            hudiDF = spark.read.format("org.apache.hudi").load(filename)
            hudiDF.createOrReplaceTempView("hudi_table")

            result_df = spark.sql("""
                SELECT vin, realtime_status, notifyflag, 
                       max(notification_timestamp) AS notification_timestamp 
                FROM hudi_table 
                WHERE notifyflag = 1 
                GROUP BY vin, realtime_status, notifyflag
                HAVING notification_timestamp = max(notification_timestamp)
            """)

            return result_df.toPandas()

        except Exception as e:
            print("Failed to read from Hudi:", e)
            return pd.DataFrame(columns=['vin', 'realtime_status', 'notifyflag', 'notification_timestamp'])
