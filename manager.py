"""
	Author:  thangbk2209
	Project: workload_dataset_extractor
	Created: 03/09/19 18:10
	Purpose:
"""

import os

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('TkAgg')
from matplotlib import pyplot

from config import *
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.session import SparkSession as spark
from pyspark.sql.types import *

sc = SparkContext(appName="Task_usage")
sql_context = SQLContext(sc)


input_data_schema = StructType([StructField('start_time', StringType(), True),
                          StructField('end_time', StringType(), True),
                          StructField('job_id', LongType(), True),
                          StructField('task_index', LongType(), True),
                          StructField('machine_id', LongType(), True),
                          StructField('mean_cpu_usage', FloatType(), True),
                          # canonical memory usage
                          StructField('canonical_memory_usage', FloatType(), True),
                          # assigned memory usage
                          StructField('assignment_memory', FloatType(), True),
                          # unmapped page cache memory usage
                          StructField('unmapped_cache_usage', FloatType(), True),
                          StructField('page_cache_usage', FloatType(), True),
                          StructField('max_mem_usage', FloatType(), True),
                          StructField('mean_disk_io_time', FloatType(), True),
                          StructField('mean_local_disk_space', FloatType(), True),
                          StructField('max_cpu_usage', FloatType(), True),
                          StructField('max_disk_io_time', FloatType(), True),
                          StructField('cpi', FloatType(), True),
                          StructField('mai', FloatType(), True),
                          StructField('sampling_portion', FloatType(), True),
                          StructField('agg_type', FloatType(), True),
                          StructField('sampled_cpu_usage', FloatType(), True)])


output_data_schema = StructType([
                         StructField('job_id', LongType(), True),
                         StructField('task_index', FloatType(), True),
                         StructField('machine_id', FloatType(), True),
                         StructField('mean_cpu_usage', FloatType(), True),
                         # canonical memory usage
                         StructField('canonical_memory_usage', FloatType(), True),
                         # assigned memory usage
                         StructField('assignment_memory', FloatType(), True),
                         # unmapped page cache memory usage
                         StructField('unmapped_cache_usage', FloatType(), True),
                         StructField('page_cache_usage', FloatType(), True),
                         StructField('max_mem_usage', FloatType(), True),
                         StructField('mean_disk_io_time', FloatType(), True),
                         StructField('mean_local_disk_space', FloatType(), True),
                         StructField('max_cpu_usage', FloatType(), True),
                         StructField('max_disk_io_time', FloatType(), True),
                         StructField('cpi', FloatType(), True),
                         StructField('mai', FloatType(), True),
                         StructField('sampling_portion', FloatType(), True),
                         StructField('agg_type', FloatType(), True),
                         StructField('sampled_cpu_usage', FloatType(), True),
                         StructField('time_stamp', FloatType(), True)])


def extract_start_time_and_end_time_of_each_part():
    '''
    extract start time, end time of each data part
    '''
    results_folder_path = '%s/min_start_max_end/' %(RESULTS_DIR)

    if not os.path.exists(results_folder_path):
        os.mkdir(results_folder_path)
    start_vs_end_time_part = None

    for file_name in os.listdir(INPUT_DIR):
        df = (
            sql_context.read
            .format('com.databricks.spark.csv')
            .schema(input_data_schema)
            .load('%s%s' % (INPUT_DIR, file_name))
        )
        df.createOrReplaceTempView('data_frame')
        query = 'SELECT min(start_time/1000000), max(end_time/1000000) from data_frame'
        _start_vs_end_time_part = sql_context.sql(query)
        _start_vs_end_time_part = _start_vs_end_time_part.toPandas().values
        if start_vs_end_time_part is None:
            start_vs_end_time_part = _start_vs_end_time_part
        else:
            start_vs_end_time_part = np.concatenate((start_vs_end_time_part, _start_vs_end_time_part))

    schema_output_df = ['start_time', 'number_of_job']
    start_vs_end_time_part = start_vs_end_time_part[start_vs_end_time_part[:,0].argsort()]
    start_vs_end_time_part_df = pd.DataFrame(start_vs_end_time_part)
    start_vs_end_time_part_df.to_csv('%s/all_job.csv' % (results_folder_path),
                                             index=False, header=None)


def extract_start_time_and_end_time_with_job_id(job_id):
    '''
    Extract start time, end time of job_id on each part 
    '''
    
    results_folder_path = '%s/min_start_max_end_all/%s' % (RESULTS_DIR, job_id)

    start_vs_end_time_with_job_id_all_part = []

    for file_name in os.listdir(INPUT_DIR):
        df = (
            sql_context.read
            .format('com.databricks.spark.csv')
            .schema(input_data_schema)
            .load('%s%s' % (INPUT_DIR, file_name))
        )
        df.createOrReplaceTempView('data_frame')
        query = 'SELECT min(start_time/1000000) as st, max(end_time/1000000) as et from data_frame where job_id = %s'\
            % (job_id)
        start_vs_end_time_with_job_id = sql_context.sql(query)
        start_vs_end_time_with_job_id = start_vs_end_time_with_job_id.toPandas().values
        start_vs_end_time_with_job_id_all_part.append(start_vs_end_time_with_job_id[0])
    start_vs_end_time_with_job_id_all_part = np.array(start_vs_end_time_with_job_id_all_part)

    start_vs_end_time_with_job_id.toPandas().to_csv('%s.csv' % (results_folder_path), index=False, header=None)


def extract_resource_with_job_id(job_id):
    '''
    Extract all resource information for task usage table of job_id
    '''

    results_folder_path = '%s/resources/%s' % (RESULTS_DIR, job_id)

    if not os.path.exists(results_folder_path):
        os.mkdir(results_folder_path)

    for file_name in os.listdir(INPUT_DIR):
        df = (
            sql_context.read
            .format('com.databricks.spark.csv')
            .schema(input_data_schema)
            .load('%s%s' % (INPUT_DIR, file_name))
        )
        df.createOrReplaceTempView('data_frame')
        query = 'SELECT start_time/1000000 as st, end_time/1000000, job_id, task_index, machine_id, mean_cpu_usage, '
        'canonical_memory_usage, assignment_memory, unmapped_cache_usage, page_cache_usage, max_mem_usage, '
        'mean_disk_io_time, mean_local_disk_space, max_cpu_usage, max_disk_io_time, cpi, mai, sampling_portion,'
        'agg_type, sampled_cpu_usage from data_frame where job_id = {} order by st ASC'.format(job_id)
        resource_usage = sql_context.sql()
        resource_usage.toPandas().to_csv('%s/%s' % (results_folder_path, file_name), index=False, header=None)
    sc.stop()


def extract_resource_at_each_time_point_of_all_job(extra_time):

    result_folder_path = RESULTS_DIR + '/{}/{}/'.format('resources', 'all')
    min_start_max_end_all_file_path = '%s/min_start_max_end/all_job.csv' % (RESULTS_DIR)
    min_start_max_end_all_schema = ['start_time', 'end_time']
    time_value = pd.read_csv(min_start_max_end_all_file_path, names=min_start_max_end_all_schema).values

    time_value = time_value.astype(int)
    time_value *= 1000000
    extra_time *= 1000000
    
    time_start = time_value[0][0]
    time_now = time_start
    time_end = time_value[len(time_value)-1][1]

    if not os.path.exists(result_folder_path):
        os.mkdir(result_folder_path)

    resource_data = None

    for part_idx in range(500):
        file_name = 'part-00{}-of-00500.csv'.format(str(part_idx).zfill(3))
        time_start_part = time_value[part_idx][0]
        time_end_part = time_value[part_idx][1]
        df = (
                sql_context.read
                .format('com.databricks.spark.csv')
                .schema(input_data_schema)
                .load("%s%s"%(INPUT_DIR, file_name))
            )
        df.createOrReplaceTempView("data_frame")

        query_information = 'sum(mean_cpu_usage), sum(canonical_memory_usage), sum(assignment_memory), sum(mean_disk_io_time), sum(mean_local_disk_space)'

        if part_idx != len(time_value)-1:
            next_file_name = 'part-00{}-of-00500.csv'.format(str(part_idx + 1).zfill(3))
            # Kiem tra xem phan thoi gian bat dau cua part tiep theo
            # voi thoi diem ket thuc part hien tai co bi chong lan khong
            start_time_next_part = time_value[part_idx + 1][0]
            if start_time_next_part <= time_end_part:
                next_df = (
                    sql_context.read
                    .format('com.databricks.spark.csv')
                    .schema(input_data_schema)
                    .load("%s%s"%(INPUT_DIR, next_file_name))
                )
                next_df.createOrReplaceTempView("next_data_frame")

                for time_stamp in range(time_now, time_end, extra_time):
                    time_stamp_arr = np.array([[time_stamp]])
                    if time_stamp >= time_end_part:
                        time_now = time_stamp
                        break
                    elif time_stamp < start_time_next_part:

                        query = 'SELECT %s from data_frame where start_time <= %s and end_time > %s'\
                            %(query_information, time_stamp, time_stamp)
                        _resource_data = sql_context.sql(query)
                        _resource_data = _resource_data.toPandas().values
                        _resource_data = np.concatenate((_resource_data, time_stamp_arr), axis=1)

                        if resource_data is None:
                            resource_data = _resource_data
                        else:
                            resource_data = np.concatenate((resource_data, _resource_data), axis=0)

                    elif time_stamp >= start_time_next_part and time_stamp < time_end_part:
                        print(time_stamp, start_time_next_part)
                        # Resource from current part
                        query_current_part = 'SELECT %s from data_frame where start_time <= %s and end_time > %s'\
                            %(query_information, time_stamp, time_stamp)
                        prev_resource_data = sql_context.sql(query_current_part)
                        prev_resource_data = prev_resource_data.toPandas().values
                        prev_resource_data = np.concatenate((prev_resource_data, time_stamp_arr), axis=1)
                        resource_data = np.concatenate((resource_data, prev_resource_data), axis=0)

                        # Resource from next part
                        query_next_part = 'SELECT %s from next_data_frame where start_time <= %s and end_time > %s'\
                            %(query_information, time_stamp, time_stamp)
                        next_resource_data = sql_context.sql(query_next_part)
                        next_resource_data = next_resource_data.toPandas().values
                        next_resource_data = np.concatenate((next_resource_data, time_stamp_arr), axis=1)
                        resource_data = np.concatenate((resource_data, next_resource_data), axis=0)
            else:
                for time_stamp in range(time_now, time_end, extra_time):
                    time_stamp_arr = np.array([[time_stamp]])
                    if time_stamp >= time_end_part:
                        time_now = time_stamp
                        break
                    else:
                        query = 'SELECT %s from data_frame where start_time <= %s and end_time > %s'\
                            %(query_information, time_stamp, time_stamp)
                        _resource_data = sql_context.sql(query)
                        _resource_data = resource_data.toPandas().values
                        _resource_data = np.concatenate((_resource_data, time_stamp_arr), axis=1)
                        resource_data = np.concatenate((resource_data, _resource_data), axis=0)
        else:
            for time_stamp in range(time_now, time_end, extra_time):
                time_stamp_arr = np.array([[time_stamp]])
                query = 'SELECT %s from data_frame where start_time <= %s and end_time > %s'\
                            %(query_information, time_stamp, time_stamp)
                _resource_data = sql_context.sql(query)
                
                _resource_data = _resource_data.toPandas().values
                print(_resource_data.shape, time_stamp_arr.shape)
                _resource_data = np.concatenate((_resource_data, time_stamp_arr), axis=1)
                resource_data = np.concatenate((resource_data, _resource_data), axis=0)
    print('====== resource data information =======')
    print(resource_data.shape)
    resource_df = pd.DataFrame(resource_data)
    resource_data_file_path = '%s/%s_ms.csv'\
        %(result_folder_path, extra_time)
    resource_df.to_csv(resource_data_file_path, index=False, header=None)
    sc.stop()


def extract_resource_at_each_time_point_with_job_id(job_id, extra_time):

    result_folder_path = RESULTS_DIR + '/{}/{}/'.format('resources', job_id)
    min_start_max_end_all_file_path = '%s/min_start_max_end/all_job.csv' % (RESULTS_DIR)
    min_start_max_end_all_schema = ['start_time', 'end_time']
    time_value = pd.read_csv(min_start_max_end_all_file_path, names=min_start_max_end_all_schema).values

    time_value = time_value.astype(int)
    time_value *= 1000000
    extra_time *= 1000000
    
    time_start = time_value[0][0]
    time_now = time_start
    time_end = time_value[len(time_value)-1][1]

    if not os.path.exists(result_folder_path):
        os.mkdir(result_folder_path)

    resource_data = None

    for part_idx in range(500):
        file_name = 'part-00{}-of-00500.csv'.format(str(part_idx).zfill(3))
        time_start_part = time_value[part_idx][0]
        time_end_part = time_value[part_idx][1]
        df = (
            sql_context.read
            .format('com.databricks.spark.csv')
            .schema(input_data_schema)
            .load("%s%s"%(INPUT_DIR, file_name))
        )
        df.createOrReplaceTempView("data_frame")
        query_information = 'sum(mean_cpu_usage), sum(canonical_memory_usage), sum(assignment_memory), sum(mean_disk_io_time), sum(mean_local_disk_space)'
        if part_idx != len(time_value)-1:
            next_file_name = 'part-00{}-of-00500.csv'.format(str(part_idx + 1).zfill(3))
            # Kiem tra xem phan thoi gian bat dau cua part tiep theo
            # voi thoi diem ket thuc part hien tai co bi chong lan khong
            start_time_next_part = time_value[part_idx + 1][0]
            if start_time_next_part <= time_end_part:
                next_df = (
                    sql_context.read
                    .format('com.databricks.spark.csv')
                    .schema(input_data_schema)
                    .load("%s%s"%(INPUT_DIR, next_file_name))
                )
                next_df.createOrReplaceTempView("next_data_frame")

                for time_stamp in range(time_now, time_end, extra_time):
                    time_stamp_arr = np.array([[time_stamp]])
                    if time_stamp >= time_end_part:
                        time_now = time_stamp
                        break
                    elif time_stamp < start_time_next_part:

                        query = 'SELECT %s from data_frame where job_id = %s and start_time <= %s and end_time > %s'\
                            %(query_information, job_id, time_stamp, time_stamp)
                        _resource_data = sql_context.sql(query)
                        _resource_data = _resource_data.toPandas().values
                        _resource_data = np.concatenate((_resource_data, time_stamp_arr), axis=1)

                        if resource_data is None:
                            resource_data = _resource_data
                        else:
                            resource_data = np.concatenate((resource_data, _resource_data), axis=0)

                    elif time_stamp >= start_time_next_part and time_stamp < time_end_part:
                        print(time_stamp, start_time_next_part)
                        # Resource from current part
                        query_current_part = 'SELECT %s from data_frame where job_id = %s and start_time <= %s and end_time > %s'\
                            %(query_information, job_id, time_stamp, time_stamp)
                        prev_resource_data = sql_context.sql(query_current_part)
                        prev_resource_data = prev_resource_data.toPandas().values
                        prev_resource_data = np.concatenate((prev_resource_data, time_stamp_arr), axis=1)
                        resource_data = np.concatenate((resource_data, prev_resource_data), axis=0)

                        # Resource from next part
                        query_next_part = 'SELECT %s from next_data_frame where job_id = %s and start_time <= %s and end_time > %s'\
                            %(query_information, job_id, time_stamp, time_stamp)
                        next_resource_data = sql_context.sql(query_next_part)
                        next_resource_data = next_resource_data.toPandas().values
                        next_resource_data = np.concatenate((next_resource_data, time_stamp_arr), axis=1)
                        resource_data = np.concatenate((resource_data, next_resource_data), axis=0)
            else:
                for time_stamp in range(time_now, time_end, extra_time):
                    time_stamp_arr = np.array([[time_stamp]])
                    if time_stamp >= time_end_part:
                        time_now = time_stamp
                        break
                    else:
                        query = 'SELECT %s from data_frame where job_id = %s and start_time <= %s and end_time > %s'\
                            %(query_information, job_id, time_stamp, time_stamp)
                        _resource_data = sql_context.sql(query)
                        _resource_data = resource_data.toPandas().values
                        _resource_data = np.concatenate((_resource_data, time_stamp_arr), axis=1)
                        resource_data = np.concatenate((resource_data, _resource_data), axis=0)
        else:
            for time_stamp in range(time_now, time_end, extra_time):
                time_stamp_arr = np.array([[time_stamp]])
                query = 'SELECT %s from data_frame where job_id = %s and start_time <= %s and end_time > %s'\
                            %(query_information, job_id, time_stamp, time_stamp)
                _resource_data = sql_context.sql(query)
                
                _resource_data = _resource_data.toPandas().values
                print(_resource_data.shape, time_stamp_arr.shape)
                _resource_data = np.concatenate((_resource_data, time_stamp_arr), axis=1)
                resource_data = np.concatenate((resource_data, _resource_data), axis=0)
    print('====== resource data information =======')
    print(resource_data.shape)
    resource_df = pd.DataFrame(resource_data)
    resource_data_file_path = '%s/%s_ms.csv'\
        %(result_folder_path, extra_time)
    resource_df.to_csv(resource_data_file_path, index=False, header=None)
    sc.stop()


def sum_resource_of_job_id(job_id):
    extra_time = 300

    resource_timestamp_folder_path = RESULTS_DIR + '/{}/{}_seconds/'.format(job_id, extra_time)
    result_folder_path = RESULTS_DIR + '/{}/{}_seconds/'.format(job_id, extra_time)
    for file_name in os.listdir(resource_timestamp_folder_path):
        df = (
            sql_context.read
            .format('com.databricks.spark.csv')
            .schema(input_data_schema)
            .load("%s%s"%(resource_timestamp_folder_path,file_name))
        )

        df.createOrReplaceTempView("data_frame")
        query = 'SELECT job_id, count(task_index), count(machine_id),sum(mean_cpu_usage),sum(canonical_memory_usage), '
        'sum(assignment_memory),sum(unmapped_cache_usage),sum(page_cache_usage), sum(max_mem_usage), '
        'sum(mean_disk_io_time), sum(mean_local_disk_space),sum(max_cpu_usage), sum(max_disk_io_time), sum(cpi), '
        'sum(mai), sum(sampling_portion), sum(agg_type), sum(sampled_cpu_usage) from data_frame group by job_id'

        resource_df = sql_context.sql(query)
        
        resource_df.toPandas().to_csv('%s/%s'%(result_folder_path, file_name), index=False, header=None)
    sc.stop()


def fix_error_and_sort_data():
    '''
    execute nan data and add time stamp column data
    '''
    pass

def extract_all_job_id_information():
    '''
    Extract all job id, number of records and number of running day of them
    '''

    # Extract all job_id
    job_id_unique = None
    job_id_appear_count = []
    i = 0
    for file_name in os.listdir(INPUT_DIR):
        i += 1
        df = (
            sql_context.read
            .format('com.databricks.spark.csv')
            .schema(input_data_schema)
            .load('%s%s' % (INPUT_DIR, file_name))
        )
        df.createOrReplaceTempView('data_frame')
        query = 'SELECT job_id from data_frame'
        _job_id_in_part = sql_context.sql(query)
        _job_id_in_part = _job_id_in_part.toPandas().values

        _job_id_in_part = np.reshape(_job_id_in_part, (_job_id_in_part.shape[0]))

        _job_id_unique, _job_id_count = np.unique(_job_id_in_part, return_counts=True)

        _job_id_appear_count = dict(zip(_job_id_unique, _job_id_count))
        
        if job_id_unique is None:
            job_id_unique = _job_id_unique
        else:
            job_id_unique = np.append(job_id_unique, _job_id_unique)

        job_id_appear_count.append(_job_id_appear_count)
    job


if __name__ == "__main__":
    extra_time = 600
    # job_id = 6176858948
    # extract_resource_with_job_id(job_id)
    # extract_start_time_and_end_time_with_job_id(job_id, extra_time)
    # extract_all_job_id_information()
    # extract_start_time_and_end_time_of_each_part()
    extract_resource_at_each_time_point_of_all_job(extra_time)
