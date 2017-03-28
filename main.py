from pyspark import SparkContext
from pyspark.sql import SQLContext

from date_utils import *
from extract import *
from transform import *
from load import *
from math_utils import *

import os

def mapCrashes(row):
    """
    Applied to an RDD, this mapping function returns a tuple of seven elements for each Row of the dataframe:
        - has_multiple_crashes: [True/False] whether the client_id has more than a single crash in its history
        - total_ssl_between_crashes: [float/None] number of hours between the two most recent crashes
        - days_between_crashes: [int/None] number of days between the two most recent crashes
        - has_main_crash: [True/False] whether the client_id has experienced a main crash in its history
        - has_content_crash: [True/False] whether the client_id has experienced a content crash in its history
        - has_plugin_crash: [True/False] whether the client_id has experienced a plugin crash in its history
        - first_crash_type: [string] type of first (latest) crash ("cssm"/"cdc"/"cdpgmp")

    @params:
        row: [Row] a row from a longitudinal RDD that includes:
            - sd: submission_date
            - ssl: subsession_length
            - cssm: crash_submit_success_main
            - cdc: crash_detected_main
            - cdpgmp: crash_detected_plugin + crash_detected_gmplugin

    @logic:
        For all users that experienced a crash during the week:
        - find the day of the most recent crash
        - if there are multiple crashes on that day, the time between crashes is 0 days and 0 hours
        - if there is only one crash on that day
            - find the day of the next most recent crash
                - if there is one, then get the number of days inbetween crashes, and the sum of all
                    subsession lengths for that period
                - if there is no previous crash in the history for that profile, we cannot measure
                    time between crashes
    """

    # return the sum of all crashes for a given submission date index
    def sumCrashes(row, index):
        return sum([row.cssm[index], row.cdc[index], row.cdpgmp[index]])

    # return boolean whether there was a crash for a given submission date index
    def isCrash(row, index):
        return sumCrashes(row, index) > 0

    # sort row lists by submission_date
    def sort_row(row):
        zipped = sorted(zip(row.sd, row.cdc, row.cssm, row.cdpgmp, row.ssl), reverse=True)
        sd, cdc, cssm, cdpgmp, ssl = zip(*zipped)
        return Row(cid=row.cid,
                   sd=list(sd),
                   ssl=list(ssl),
                   cdc=list(cdc),
                   cssm=list(cssm),
                   cdpgmp=list(cdpgmp))

    # return the sum of all crashes by type (tuple (cssm, cdc, cdpgmp))
    def sumCrashesByType(row):
        return (sum(row.cssm), sum(row.cdc), sum(row.cdpgmp))

    # return type of crash at index
    ## returns main if main + other, content if content + plugin
    def typeOfCrash(row, index):
        if row.cssm[index] > 0:
            return "cssm"
        if row.cdc[index] > 0:
            return "cdc"
        if row.cdpgmp[index] > 0:
            return "cdpgmp"
        return "none"

    first_crash = None                   # submission date index of first (latest) crash
    next_crash = None                    # submission date index of second (second latest) crash

    has_multiple_crashes = False         # whether user has a previous crash
    total_ssl_between_crashes = None     # total subsession length between crashes

    has_main_crash = False               # whether user has experienced a main crash
    has_content_crash = False            # whether user has experienced a content crash
    has_plugin_crash = False             # whether user has experienced a plugin crash

    first_crash_type = None              # type of first (latest) crash [main, content, plugin]

    # sort the row by submission_date
    sorted_row = sort_row(row)

    # find counts of crashes
    sum_main, sum_content, sum_plugin = sumCrashesByType(sorted_row)
    if sum_main > 0:
        has_main_crash = True
    if sum_content > 0:
        has_content_crash = True
    if sum_plugin > 0:
        has_plugin_crash = True

    # iterate through all subsessions (brute force):
    for index, submission_date in enumerate(sorted_row.sd):
        # if a first crash was already found, then look for a second crash
        if first_crash is not None:
            if isCrash(sorted_row, index):
                next_crash = index
                break
        # if there were no previous crash, look for the first one
        elif isCrash(sorted_row, index):
            first_crash = index
            first_crash_type = typeOfCrash(sorted_row, index)
            # if there was more than one crash that day
            if sumCrashes(sorted_row, index) > 1:
                next_crash = index
                break

    # if there were more than one crash in total for that client_id
    if next_crash is not None:
        has_multiple_crashes = True
        s = slice(first_crash, next_crash)
        total_ssl_between_crashes = sum(sorted_row.ssl[s]) / 60. / 60 # converted to hours

    return (has_multiple_crashes,      # True/False
            total_ssl_between_crashes, # in hours
            has_main_crash,            # True/False
            has_content_crash,         # True/False
            has_plugin_crash,          # True/False
            first_crash_type)          # cssm/cdc/cdpgmp

def main_alg():
    """
    This function ties everything together.
    The analysis is done for however many days since the last run (obtained by parsing available JSON files in the same directory).
    If no such files are found, the anaysis is run since September 1st 2016.
    """
    
    # setup spark
    sc = SparkContext(appName="FirefoxCrashGraphs")
    sqlContext = SQLContext(sc)

    setup_load(sqlContext)
    setup_extract(sqlContext)
    setup_transform(sqlContext)

    # read and clean data; save as SQL table
    print "***** READING DATA...",
    filteredPingsDF = read_main_summary()
    filteredPingsDF_str = "filteredPingsDF"
    sqlContext.registerDataFrameAsTable(filteredPingsDF, filteredPingsDF_str)
    print "DONE!"

    # create aggregate table by client_id and submission_date; save as SQL table
    print "***** CREATING AGGREGATE TABLE...",
    aggregateDF = aggregate_by_client_date_e10s(filteredPingsDF_str)
    aggregateDF_str = "aggregateDF"
    sqlContext.registerDataFrameAsTable(aggregateDF, aggregateDF_str)
    print "DONE!"

    # get start and end dates
    last_date = find_last_date()
    start_backfill = last_date + timedelta(days=1)
    end_backfill = date.today() - timedelta(days=1)

    # get date ranges
    dates = [] # --list of all end_dates in period
    print "***** FINDING ALL Mondays, Wednesdays, Fridays BETWEEN {start} AND {end}..."\
           .format(start=start_backfill, end=end_backfill),

    # get all M, W, F between the two provided dates
    delta = end_backfill - start_backfill
    for i in range(delta.days + 1):
        day = end_backfill - timedelta(days=i)
        if day.weekday() in [0,2,4]:
            end_date = day
            start_date = day - timedelta(days=6)
            dates.append( (start_date, end_date) )
    print "{} DATES".format(len(dates))

    # loop through all dates
    for i, d in enumerate(dates):
        print
        print "***** DATE {curr} of {tot}".format(curr=i+1, tot=len(dates))
        start_date = d[0]
        end_date = d[1]
        start_date_str = start_date.strftime("%Y%m%d")
        end_date_str = end_date.strftime("%Y%m%d")
        print "***** Week of interest: {start} :: {end}".format(start=start_date, end=end_date)

        # calculate WAU7
        wau7 = get_wau7(aggregateDF_str, start_date_str, end_date_str)
        print "\tActive users: {:,} (based on a 1% sample)".format(wau7*100)

        # calculate number of profiles that crashed
        num_profiles_crashed = get_num_crashed(aggregateDF_str, start_date_str, end_date_str)
        print "\tNumber of users that experienced a crash: {:,} ({:.2%} of active profiles)"\
               .format(num_profiles_crashed*100, float(num_profiles_crashed) / wau7)

        # calculate new profiles and proportion crashed
        num_new_profiles = get_num_new_profiles(aggregateDF_str, start_date_str, end_date_str)
        print "\tNew profiles: {:,} (based on a 1% sample) ({:.2%} of active profiles)".format(num_new_profiles*100,
                                                                                               float(num_new_profiles)/wau7)
        num_new_profiles_crashed = get_num_new_profiles_crashed(aggregateDF_str, start_date_str, end_date_str)
        print "\tNumber of new users that experienced a crash: {:,} ({:.2%} of new profiles)"\
               .format(num_new_profiles_crashed*100, float(num_new_profiles_crashed) / num_new_profiles)

        # get subset of aggregated dataframe containing only the pings for profiles that crashed
        aggregate_crashed = aggregate_subset(aggregateDF_str, start_date_str, end_date_str)

        # transform into longitudinal format
        crashed_longitudinal = make_longitudinal(aggregate_crashed)

        # apply mapping function
        crash_statistics = crashed_longitudinal.rdd.map(mapCrashes)

        # get counts of crashed user types
        crash_statistics_counts = crash_statistics.countByKey()
        print "\tNumber of profiles that crashed for the first time: {:,} ({:.2%} of crashed profiles)"\
               .format(crash_statistics_counts[False]*100,
                      float(crash_statistics_counts[False])/num_profiles_crashed)
        print "\tNumber of profiles that crashed and had a previous crash: {:,} ({:.2%} of crashed profiles)"\
               .format(crash_statistics_counts[True]*100,
                       float(crash_statistics_counts[True])/num_profiles_crashed)

        # calculate counts for e10s
        e10s_counts = get_e10s_counts(aggregateDF_str, start_date_str, end_date_str)
        print "\tNumber of profiles that have e10s enabled: {:,} ({:.2%} of crashed profiles)"\
              .format(e10s_counts[0]*100, float(e10s_counts[0])/num_profiles_crashed)
        print "\tNumber of profiles that have e10s disabled: {:,} ({:.2%} of crashed profiles)"\
              .format(e10s_counts[1]*100, float(e10s_counts[1])/num_profiles_crashed)

        # calculate crash rates
        crash_rates_avg_by_user = get_crash_rates_by_user(aggregateDF_str, start_date_str, end_date_str)
        print "\tMain crashes per hour: {}".format(crash_rates_avg_by_user[0]*1000)
        print "\tContent crashes per hour: {}".format(crash_rates_avg_by_user[1]*1000)
        print "\tPlugin crashes per hour: {}".format(crash_rates_avg_by_user[2]*1000)

        # calculate crash rates by e10s status
        crash_rates_avg_by_user_and_e10s = get_crash_rates_by_user_and_e10s(aggregateDF_str, start_date_str, end_date_str)
        print "\tMain crashes per hour (e10s enabled): {}".format(crash_rates_avg_by_user_and_e10s[0]*1000)
        print "\tContent crashes per hour (e10s enabled): {}".format(crash_rates_avg_by_user_and_e10s[1]*1000)
        print "\tPlugin crashes per hour (e10s enabled): {}".format(crash_rates_avg_by_user_and_e10s[2]*1000)
        print "\tMain crashes per hour (e10s disabled): {}".format(crash_rates_avg_by_user_and_e10s[3]*1000)
        print "\tContent crashes per hour (e10s disabled): {}".format(crash_rates_avg_by_user_and_e10s[4]*1000)
        print "\tPlugin crashes per hour (e10s disabled): {}".format(crash_rates_avg_by_user_and_e10s[5]*1000)

        # get crash statistics
        print "***** SAVING CRASH DATA TO JSON...",
        crash_statistics_pd = RDD_to_pandas(crash_statistics, "has_multiple_crashes = True", ["total_ssl_between_crashes"])
        write_col_json("fx_crashgraphs_hours", crash_statistics_pd.total_ssl_between_crashes, "hours",
                       start_date_str, end_date_str)
        print "DONE!"

        # get summary statistics
        print "***** SAVING RESULTS TO JSON...",
        crash_statistics_pd = RDD_to_pandas(crash_statistics) #TODO: combine the two to_pandas operations into one. This operation is expensive.
        summary = make_dict_results(end_date, wau7, num_new_profiles, num_profiles_crashed, num_new_profiles_crashed,
                                    crash_statistics_counts, crash_rates_avg_by_user, crash_rates_avg_by_user_and_e10s,
                                    crash_statistics_pd, e10s_counts)
        write_dict_json("fx_crashgraphs", summary, start_date_str, end_date_str)
        print "DONE!"

        print
        print "**** MERGING SUMMARY JSON FILES...",
        # merge summary JSON files into one
        os.system('jq -c -s "[.[]|.[]]" fx_crashgraphs-*.json > "fx_crashgraphs.json"')
        print "DONE!"

if __name__ == '__main__':
    main_alg()
