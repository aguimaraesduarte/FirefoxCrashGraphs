from pyspark import SparkContext
from pyspark.sql import SQLContext

from date_utils import *
from extract import *
from transform import *
from load import *
from math_utils import *

import os

S3_BUCKET_NAME = "mozilla-metrics"
S3_PATH = "sguha/crashgraphs/JSON/"

def main_alg():
    """
    This function ties everything together.
    The analysis is done for however many days since the last run (obtained by parsing available JSON files in the same directory).
    If no such files are found, the anaysis is run since September 1st 2016.
    """
    
    # setup sparkContext
    sc = SparkContext(appName="FirefoxCrashGraphs")
    sc.addPyFile('date_utils.py')
    sc.addPyFile('math_utils.py')
    sc.addPyFile('extract.py')
    sc.addPyFile('load.py')
    sc.addPyFile('transform.py')

    # setup sqlContext
    sqlContext = SQLContext(sc)
    setup_load(sqlContext)
    setup_extract(sqlContext)
    setup_transform(sqlContext)

    # fetch files from S3
    print "***** FETCHING FILES FROM S3...",
    fetch_latest_from_s3(S3_BUCKET_NAME, S3_PATH)
    print "DONE!"

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
    print "***** FINDING ALL WEEKDAYS BETWEEN {start} AND {end}..."\
           .format(start=start_backfill, end=end_backfill),

    # get all weekdays between the two provided dates
    delta = end_backfill - start_backfill
    for i in range(delta.days + 1):
        day = end_backfill - timedelta(days=i)
        if day.weekday() in [0,1,2,3,4]:
            end_date = day
            start_date = day - timedelta(days=6)
            dates.append( (start_date, end_date) )
    print "{} DATES".format(len(dates))

    # loop through all dates
    for i, d in enumerate(reversed(dates)):
        print
        print "***** DATE {curr} of {tot}".format(curr=i+1, tot=len(dates))
        start_date = d[0]
        end_date = d[1]
        start_date_str = start_date.strftime("%Y%m%d")
        end_date_str = end_date.strftime("%Y%m%d")
        print "***** Week of interest: {start} :: {end}".format(start=start_date, end=end_date)

        # calculate WAU7
        wau7 = get_wau7(aggregateDF_str, start_date_str, end_date_str)
        print "\tActive profiles: {:,} (based on a 1% sample)".format(wau7*100)

        # calculate number of profiles that crashed
        num_profiles_crashed, num_profiles_crashed_2 = get_num_crashed(aggregateDF_str, start_date_str, end_date_str) # TODO: save to list
        print "\tNumber of profiles that experienced a crash: {:,} ({:.2%} of active profiles)"\
               .format(num_profiles_crashed*100, float(num_profiles_crashed) / wau7)
        print "\tNumber of profiles that experienced >= 2 crashes: {:,} ({:.2%} of active profiles)"\
               .format(num_profiles_crashed_2*100, float(num_profiles_crashed_2) / wau7)

        # calculate new profiles and proportion crashed
        num_new_profiles = get_num_new_profiles(aggregateDF_str, start_date_str, end_date_str)
        print "\tNew profiles: {:,} (based on a 1% sample) ({:.2%} of active profiles)".format(num_new_profiles*100,
                                                                                               float(num_new_profiles)/wau7)
        num_new_profiles_crashed, num_new_profiles_crashed_2 = get_num_new_profiles_crashed(aggregateDF_str, start_date_str, end_date_str) # TODO: save to list
        print "\tNumber of new profiles that crashed 1+ times: {:,} ({:.2%} of new profiles)"\
               .format(num_new_profiles_crashed*100, float(num_new_profiles_crashed) / num_new_profiles)
        print "\tNumber of new profiles that crashed 2+ times: {:,} ({:.2%} of new profiles)"\
               .format(num_new_profiles_crashed_2*100, float(num_new_profiles_crashed_2) / num_new_profiles)

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


        ##### start of new profiles (3 weeks ago) that had 2 weeks to crash
        # get subset of aggregated dataframe containing only the pings for profiles that were created 2 weeks prior
        aggregate_new = aggregate_new_users(aggregateDF_str, start_date_str, end_date_str)
        new_longitudinal = make_longitudinal_new(aggregate_new)
        new_statistics = new_longitudinal.rdd.map(mapCrashes_new)

        # get counts of new user types
        new_statistics_counts = new_statistics.countByKey()
        new__crashed = new_statistics_counts[1]
        new_tot = new_statistics_counts[0]+new_statistics_counts[1]
        print "\tNew profiles created between {} and {}: {:,}"\
              .format((str2date(end_date_str)-timedelta(days=19)).isoformat(),
                      (str2date(end_date_str)-timedelta(days=13)).isoformat(),
                      new_tot*100)
        print "\tNumber of new profiles that crashed 1+ times within 2 weeks of profile creation: {:,} ({:.2%} of new users)"\
               .format(new_1*100, float(new_1)/new_tot)
        ##### end of new profiles (3 weeks ago) that had 2 weeks to crash

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
                       start_date_str, end_date_str, S3_BUCKET_NAME, S3_PATH, 1)
        print "DONE!"

        # get summary statistics
        print "***** SAVING RESULTS TO JSON...",
        crash_statistics_pd = RDD_to_pandas(crash_statistics) #TODO: combine the two to_pandas operations into one. This operation is expensive.
        summary = make_dict_results(end_date, wau7, num_new_profiles, num_profiles_crashed, num_profiles_crashed_2,
                                    num_new_profiles_crashed, num_new_profiles_crashed_2,
                                    crash_statistics_counts, crash_rates_avg_by_user, crash_rates_avg_by_user_and_e10s,
                                    crash_statistics_pd, e10s_counts, new_statistics_counts) # TODO: make all the repeat variables lists
        write_dict_json("fx_crashgraphs", summary, start_date_str, end_date_str, S3_BUCKET_NAME, S3_PATH, 1)
        print "DONE!"

        print "***** MERGING SUMMARY JSON FILES...",
        # merge summary JSON files into one
        os.system('jq -c -s "[.[]|.[]]" fx_crashgraphs-*.json > "fx_crashgraphs.json"')
        store_latest_on_s3(S3_BUCKET_NAME, S3_PATH, "fx_crashgraphs.json")
        print "DONE!"

    print
    # remove local json files
    #os.system('rm *.json')
    print "DONE!"

if __name__ == '__main__':
    main_alg()
