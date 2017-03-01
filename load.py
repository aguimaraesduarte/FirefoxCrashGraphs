import os.path
from datetime import date, datetime, timedelta
from collections import Counter

import numpy as np
import ujson as json

from transform import *
from math_utils import *

sqlContext = None

def setup_load(sqlC):
    global sqlContext
    sqlContext = sqlC

def write_col_json(fn, pandas_col, interval_str, start_date_str, end_date_str):
    """
    This function writes the counter from a column from a pandas dataframe to a json file.

    @params:
        fn: [tring] file name of output json
        pandas_col: [Series] column of the pandas dataframe to create counter
        interval_str: [string] name of interval (ex: "days", "hours"...)
        start_date_str: [string] start date to append to file name
        end_date_str: [string] end date to append to file names
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

def write_dict_json(fn, res_data, start_date_str, end_date_str):
    """
    This function writes the content of a dictionary to a json file.

    @params:
        fn: [string] file name of output json
        res_data: [dict] dictionary object with summary data
        start_date_str: [string] start date to append to file name
        end_date_str: [string] end date to append to file names
    """

    suffix = "-" + start_date_str + "-" + end_date_str
    file_name = fn + suffix + ".json"

    if os.path.exists(file_name):
        print "{} exists, we will overwrite it.".format(file_name)

    # res_data is a JSON object.
    json_entry = json.dumps(res_data)

    with open(file_name, "w") as json_file:
        json_file.write("[" + json_entry.encode('utf8') + "]\n")

def make_dict_results(end_date, wau7, num_new_profiles, num_profiles_crashed, num_new_profiles_crashed,
                      crash_statistics_counts, crash_rates_avg_by_user, crash_rates_avg_by_user_and_e10s,
                      df_pd, e10s_counts):
    """
    This function returns a dictionary with a summary of the results to output a json file.

    @params:
        keys to save as a dictionary. Self-explanatory
        df_pd: [pandas DF] Dataframe with crash data histograms
    """

    counts_tot, counts_mult, counts_first = getCountsLastCrashes(df_pd)

    return {
        "date": end_date.strftime("%Y-%m-%d"),
        "proportion_wau_crashes": (1.0*num_profiles_crashed)/wau7,
        "proportion_new_profiles": (1.0*num_new_profiles)/wau7,
        "proportion_first_time_crashes": (1.0*crash_statistics_counts[False])/num_profiles_crashed,
        "proportion_multiple_crashes": (1.0*crash_statistics_counts[True])/num_profiles_crashed,
        "proportion_new_crashes": (1.0*num_new_profiles_crashed)/num_new_profiles,
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
        "prop_last_crash_main_tot": counts_tot.cssm,
        "prop_last_crash_content_tot": counts_tot.cdc,
        "prop_last_crash_plugin_tot": counts_tot.cdpgmp,
        "prop_last_crash_main_mult": counts_mult.cssm,
        "prop_last_crash_content_mult": counts_mult.cdc,
        "prop_last_crash_plugin_mult": counts_mult.cdpgmp,
        "prop_last_crash_main_first": counts_first.cssm,
        "prop_last_crash_content_first": counts_first.cdc,
        "prop_last_crash_plugin_first": counts_first.cdpgmp
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
