import os
from pandas import read_csv
import numpy as np

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.session import SparkSession as spark
from pyspark.sql.types import *

sc = SparkContext(appName="Task_usage")
sql_context = SQLContext(sc)

input_data_schema = StructType([StructField('timestamp', LongType(), True),
                          StructField('vm_id', StringType(), True),
                          StructField('min_cpu', FloatType(), True),
                          StructField('max_cpu', FloatType(), True),
                          StructField('avg_cpu', DoubleType(), True)])

# output_data_schema = StructType([
#                          StructField('timestamp', LongType(), True),
#                          StructField('sum_cpu', DoubleType(), True)])

total_file = 1

colnames = ['timestamp', 'cpu']
usecols = [0, 4]

# results_folder_path = '%s/' % (RESULTS_DIR)
INPUT_DIR = '/root/thangbk2209/thesis/datasets/azure/'
results_folder_path = '/root/thangbk2209/thesis/datasets/azure_resource'

start_vs_end_time_with_job_id_all_part = []
all_resource_data = None
for file_name in os.listdir(INPUT_DIR):
    df = (
        sql_context.read
        .format('com.databricks.spark.csv')
        .schema(input_data_schema)
        .load('%s%s' % (INPUT_DIR, file_name))
    )
    df.createOrReplaceTempView('data_frame')

    query = 'SELECT distinct timestamp from data_frame'
    # print(query)
    time_stamp_extract = sql_context.sql(query)
    time_stamp_extract = time_stamp_extract.toPandas().values
    extract_data_file = []
    for _timestamp_extract in time_stamp_extract:

        _timestamp_value = _timestamp_extract[0]
        
        query = 'SELECT sum(avg_cpu) from data_frame where timestamp = %s'% (_timestamp_value)
        print(query)
        data_extract = sql_context.sql(query)
        data_extract = data_extract.toPandas().values
        timestamp_arr = np.array([[_timestamp_value]])

        data = np.concatenate((timestamp_arr, data_extract), axis=1)
        extract_data_file.append(data[0])
        # start_vs_end_time_with_job_id_all_part.append(start_vs_end_time_with_job_id[0])
        # start_vs_end_time_with_job_id_all_part = np.array(start_vs_end_time_with_job_id_all_part)

        # start_vs_end_time_with_job_id.toPandas().to_csv('%s.csv' % (results_folder_path), index=False, header=None)
    if all_resource_data is None:
        all_resource_data = np.array(extract_data_file)
    else:
        all_resource_data = np.concatenate((all_resource_data, extract_data_file), axis=1)

    all_resource_data.toPandas().to_csv('%s/%s' % (results_folder_path, file_name), index=False, header=None)
