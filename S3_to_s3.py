#pip install pyspark
#pip install boto3

#import Json
import boto3
from pyspark.sql import SparkSession

aws_access_key_id = 'YOUR_ACCESS_KEY'
aws_secret_access_key = 'YOUR_SECRET_KEY'
bucket_name = 'your-bucket-name'
s3_key = 'path/to/your/file.csv'  # or .json, .txt, etc.
local_file_name = 'downloaded_file.csv'
#Some time you also need to pass region and while creating the bucket in s3 you need to grant access
#For above credemtial you can create a seperate json file and from their you can extract a data, or you can create enviournment variable
s3 = boto3.client('s3',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key)



s3.download_file(bucket_name, s3_key, local_file_name)
print("File downloaded successfully.")


#Creating a spark session

spark = SparkSession.builder \
    .appName("S3 to Spark") \
    .getOrCreate()
#Creating a spark dataframe
df = spark.read.option("header", True).csv(local_file_name)
df.show()

df = df.repartition(4)

df.write.mode("overwrite").csv("s3a://your-bucket-name/path/to/folder")