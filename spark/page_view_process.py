from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
from datetime import datetime
import pyspark.sql.functions as F
from collections import Counter

def to_date(timestamp_ms_relative):
    TIMESTAMP_DELTA=1465876799998
    dt = datetime.fromtimestamp((int(timestamp_ms_relative)+TIMESTAMP_DELTA)//1000)
    day  = str(dt).split(' ')[0]
    hour = str(dt).split(' ')[1].split(':')[0]
    #minu = str(dt).split(' ')[1].split(':')[1]
    return day + '-' + hour

page_views_schema = StructType( [StructField("uuid", StringType(), True), StructField("document_id", IntegerType(), True), StructField("timestamp", IntegerType(), True), StructField("platform", IntegerType(), True), StructField("geo_location", StringType(), True), StructField("traffic_source", IntegerType(), True)])
path = "s3://outbrain-contest/data/page_views.csv"

if __name__ == "__main__":
    # Initialize the spark context.
    spark = SparkSession.builder.appName("feature").getOrCreate()
    AWS_KEY = ""
    AWS_SECRET = ""
    sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", AWS_KEY)
    sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", AWS_SECRET)
    page_views_df = spark.read.schema(page_views_schema).options(header='true', inferschema='false', nullValue= -99).csv(path)
    uuid_time_doc = page_views_df.select(['uuid', 'document_id', 'timestamp']).rdd.map(lambda x : ((x.asDict()['uuid'], x.asDict()['timestamp'], x.asDict()['document_id']), 1)).reduceByKey(lambda a, b: a + b)
    uuidtime_doccount = uuid_time_doc.map(lambda x : ((x[0][0], x[0][1]), {x[0][2] : x[1]})).reduceByKey(lambda x, y : dict(Counter(x)+Counter(y)))
    uuid_timedoccount = uuidtime_doccount.map(lambda x : (x[0][0], {x[0][1] : x[1]})).reduceByKey(lambda x, y :dict(x.items() + y.items() +[(k, x[k] + y[k]) for k in set(x) & set(y)]))
    df_summary = spark.createDataFrame(uuid_timedoccount, ['uuid', 'summary'])
    #uuid_doc_count = uuid_doc_count.map(lambda x : (x[0][0], {x[0][1] : x[1]})).reduceByKey(lambda x, y : dict(Counter(x)+Counter(y)))
    #uuid_hour_count = page_views_df.select(['uuid', 'timestamp']).rdd.map(lambda x : ((x.asDict()['uuid'], convert_dt(x.asDict()['timestamp']).hour), 1)).reduceByKey(lambda a, b: a + b)
    #uuid_hour_count = uuid_hour_count.map(lambda x : (x[0][0], {x[0][1] : x[1]})).reduceByKey(lambda x, y : dict(Counter(x)+Counter(y)))
    #df = spark.createDataFrame(uuid_hour_count, ['uuid', 'hour:freq'])
    #df.write.mode('overwrite').parquet(path='s3n://predictive-model/reformat/use_mean_col/dict/page_view_uuid_hourfreq')
    #uuid_platform_count = page_views_df.select(['uuid', 'platform']).rdd.map(lambda x : ((x.asDict()['uuid'], x.asDict()['platform']), 1)).reduceByKey(lambda a, b: a + b)
    #uuid_platform_count = uuid_platform_count.map(lambda x : (x[0][0], {x[0][1] : x[1]})).reduceByKey(lambda x, y : dict(Counter(x)+Counter(y)))
    #df1 = spark.createDataFrame(uuid_platform_count, ['uuid', 'platform:freq'])
    #df1.write.mode('overwrite').parquet(path='s3n://predictive-model/reformat/use_mean_col/dict/page_view_uuid_platformfreq')
