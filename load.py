import os.path
from datetime import date, datetime, timedelta
from collections import Counter

import numpy as np
import ujson as json
import boto3

from transform import *
from math_utils import *

sqlContext = None

def setup_load(sqlC):
    global sqlContext
    sqlContext = sqlC

def write_col_json(fn, pandas_col, interval_str, start_date_str, end_date_str,
                   bucket_name, s3_data_path, save_to_s3):
    """
    This function writes the counter from a column from a pandas dataframe to a json file.

    @params:
        fn: [tring] file name of output json
        pandas_col: [Series] column of the pandas dataframe to create counter
        interval_str: [string] name of interval (ex: "days", "hours"...)
        start_date_str: [string] start date to append to file name
        end_date_str: [string] end date to append to file names
        bucket_name: [string] S3 bucket name
        s3_data_path: [string] path to folder on S3
    """

    cnter = dict(Counter(pandas_col.map(np.ceil).map(int)))
    inter = cnter.keys()
    inter = [int(float(d)) for d in inter]
    counts = cnter.values()

    json_str = '[%s]'
    row_str = '{"' + interval_str + '":%s, "count":%s}'

    all_rows = []
    for i in range(len(inter)):
        all_rows.append(row_str%(inter[i], counts[i]))

    all_rows_str = ",".join(all_rows)

    json_final = json_str%all_rows_str

    suffix = "-" + start_date_str + "-" + end_date_str
    file_name = fn + suffix + ".json"

    if os.path.exists(file_name):
        print "{} exists, we will overwrite it.".format(file_name)

    with open(file_name, "w") as json_file:
        json_file.write(json_final)

    # save to S3
    if save_to_s3:
        store_latest_on_s3(bucket_name, s3_data_path, file_name)

def write_dict_json(fn, res_data, start_date_str, end_date_str,
                    bucket_name, s3_data_path, save_to_s3):
    """
    This function writes the content of a dictionary to a json file.

    @params:
        fn: [string] file name of output json
        res_data: [dict] dictionary object with summary data
        start_date_str: [string] start date to append to file name
        end_date_str: [string] end date to append to file names
        bucket_name: [string] S3 bucket name
        s3_data_path: [string] path to folder on S3
    """

    suffix = "-" + start_date_str + "-" + end_date_str
    file_name = fn + suffix + ".json"

    if os.path.exists(file_name):
        print "{} exists, we will overwrite it.".format(file_name)

    # res_data is a JSON object.
    json_entry = json.dumps(res_data)

    with open(file_name, "w") as json_file:
        json_file.write("[" + json_entry.encode('utf8') + "]\n")

    # save to S3
    if save_to_s3:
        store_latest_on_s3(bucket_name, s3_data_path, file_name)

def make_dict_results(end_date, wau7, num_new_profiles, num_profiles_crashed, num_profiles_crashed_2,
                      num_new_profiles_crashed, num_new_profiles_crashed_2, 
                      crash_statistics_counts, crash_rates_avg_by_user, crash_rates_avg_by_user_and_e10s,
                      df_pd, e10s_counts, new_statistics_counts):
    """
    This function returns a dictionary with a summary of the results to output a json file.

    @params:
        keys to save as a dictionary. Self-explanatory
        df_pd: [pandas DF] Dataframe with crash data histograms
    """
    new_crashed = new_statistics_counts[1]
    new_tot = new_statistics_counts[0]+new_statistics_counts[1]

    return {
        "date": end_date.strftime("%Y-%m-%d"),
        "proportion_wau_crashes": (1.0*num_profiles_crashed)/wau7,
        "proportion_wau_crashes_2": (1.0*num_profiles_crashed_2)/wau7,
        "proportion_new_profiles": (1.0*num_new_profiles)/wau7,
        "proportion_first_time_crashes": (1.0*crash_statistics_counts[False])/num_profiles_crashed,
        "proportion_multiple_crashes": (1.0*crash_statistics_counts[True])/num_profiles_crashed,
        "proportion_new_crashes": (1.0*num_new_profiles_crashed)/num_new_profiles,
        "proportion_new_crashes_2": (1.0*num_new_profiles_crashed_2)/num_new_profiles,
        "proportion_e10s_enabled": (1.0*e10s_counts[0])/num_profiles_crashed,
        "proportion_e10s_disabled": (1.0*e10s_counts[1])/num_profiles_crashed,
        "crash_rate_main_avg_by_user": crash_rates_avg_by_user[0]*1000,
        "crash_rate_content_avg_by_user": crash_rates_avg_by_user[1]*1000,
        "crash_rate_plugin_avg_by_user": crash_rates_avg_by_user[2]*1000,
        "crash_rate_main_avg_by_user_and_e10s_enabled": crash_rates_avg_by_user_and_e10s[0]*1000,
        "crash_rate_content_avg_by_user_and_e10s_enabled": crash_rates_avg_by_user_and_e10s[1]*1000,
        "crash_rate_plugin_avg_by_user_and_e10s_enabled": crash_rates_avg_by_user_and_e10s[2]*1000,
        "crash_rate_main_avg_by_user_and_e10s_disabled": crash_rates_avg_by_user_and_e10s[3]*1000,
        "crash_rate_content_avg_by_user_and_e10s_disabled": crash_rates_avg_by_user_and_e10s[4]*1000,
        "crash_rate_plugin_avg_by_user_and_e10s_disabled": crash_rates_avg_by_user_and_e10s[5]*1000,
        "median_hours_between_crashes": df_pd.total_ssl_between_crashes.median(),
        "geom_hours_between_crashes": geometric_mean(df_pd.total_ssl_between_crashes),
        "proportion_new_crashes_bis": (1.0*new_crashed)/new_tot
    }

def find_last_date(directory=".", start="fx_crashgraphs-", end=".json"):
    """
    This function looks through all json files in the directory and returns the latest available date with data.
    If no matching files are found, the last date returned is Sep 1st, 2016.

    @params:
        directory: [string] directory (relative path) to look for files. Default: "." current directory
        start: [string] start format of filenames. Default: "fx_crashgraphs-" weekly summary crash data
        end: [string] end format of filenames. Default: ".json" json files
    """
    # List all json files with the specified format
    all_files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))
                                                   and f.endswith(end)
                                                   and f.startswith(start)]

    # Get the last date
    # Note: this requires the use of "fx_crashgraphs-YYYYMMDD-YYYYMMDD.json" files
    # Note2: if no such files, will default to September 1st, 2016
    # TODO: use regex to make it more universal
    try:
        last_date = sorted(all_files)[-1][-13:-5] # "%Y%m%d"
    except:
        return date(2016,9,1)

    # Return last date as datetime date
    return datetime.strptime(last_date, "%Y%m%d").date()

def fetch_latest_from_s3(bucket_name, s3_data_path):
    """
    Retrieve files from S3.

    @params:
        bucket_name: [string] S3 bucket name
        s3_data_path: [string] path to folder on S3
    """

    # Connect to client
    client = boto3.client('s3', 'us-west-2')
    transfer = boto3.s3.transfer.S3Transfer(client)

    # List all file paths
    files = client.list_objects(Bucket=bucket_name)['Contents']

    # suffix
    suffix = "fx_crashgraphs-"

    # List all relevant files for CrashGraphs
    cg_files = [f['Key'] for f in files if f['Key'].startswith(s3_data_path+suffix)]

    # Fetch files
    for f in cg_files:
        transfer.download_file(bucket_name, f, f.split('/')[-1])

def store_latest_on_s3(bucket_name, s3_data_path, filename):
    """
    Save file to S3.

    @params:
        bucket_name: [string] S3 bucket name
        s3_data_path: [string] path to folder on S3
        filename: [string] name of file to save on S3 (+local)
    """

    client = boto3.client('s3', 'us-west-2')
    transfer = boto3.s3.transfer.S3Transfer(client)
    
    # Update the state in the analysis bucket.
    key_path = s3_data_path + filename
    transfer.upload_file(filename, bucket_name, key_path)
