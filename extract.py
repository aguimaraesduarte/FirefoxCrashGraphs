from date_utils import *

sqlContext = None

TODAY_INT = date2int(date.today())                        # today (day notebook is run) as days since Jan 1st, 1970
PCD_CUTOUT_START_INT = date2int(date(2010, 1, 1))  # profiles created before this date are removed (days since Jan 1st, 1970)
SD_CUTOUT_START_STR = "20160701"                        # pings submitted before this date are removed

def setup_extract(sqlC):
    global sqlContext
    sqlContext = sqlC

def read_main_summary():
    """
    This function imports the main_summary dataset from S3, selects the variables of interest,
    applies several filters, and returns the filtered dataset.
    """

    # connect to the main_summary dataset
    allPingsDF = sqlContext.read.load("s3://telemetry-parquet/main_summary/v3", "parquet", mergeSchema=True)

    # perform variable selection with column renaming
    allPingsDFSelect = allPingsDF.select(
               allPingsDF.client_id.alias("cid"),
               allPingsDF.sample_id.alias("sid"),
               allPingsDF.normalized_channel.alias("channel"),
               allPingsDF.submission_date_s3.alias("sd"),
               allPingsDF.app_name.alias("appname"),
               allPingsDF.subsession_length.alias("ssl"),
               allPingsDF.crash_submit_success_main.alias("cssm"),
               allPingsDF.crashes_detected_content.alias("cdc"),
               allPingsDF.crashes_detected_gmplugin.alias("cdgmp"),
               allPingsDF.crashes_detected_plugin.alias("cdp"),
               allPingsDF.profile_creation_date.alias("pcd"),
               allPingsDF.e10s_enabled.alias("e10s"))

    # filter, replace missing values with zeroes, and cache dataframe
    # - 1% sample (sample_id is 42)
    # - channel is release
    # - application is Firefox
    # - data was submitted since July 2016 (previous data may be missing information)
    # Dataframe is `cache`d to memory for performance improvements
    filteredPingsDF = allPingsDFSelect.filter(allPingsDFSelect.sid == "42")\
                                      .filter(allPingsDFSelect.channel == "release")\
                                      .filter(allPingsDFSelect.appname == "Firefox")\
                                      .filter(allPingsDFSelect.sd >= SD_CUTOUT_START_STR)\
                                      .fillna({"ssl": 0, "cssm":0, "cdc":0, "cdgmp":0, "cdp":0})\
                                      .filter(allPingsDFSelect.ssl >= 0)\
                                      .filter(allPingsDFSelect.ssl <= 86400)\
                                      .filter(allPingsDFSelect.pcd >= PCD_CUTOUT_START_INT)\
                                      .filter(allPingsDFSelect.pcd <= TODAY_INT)\
                                      .filter(allPingsDFSelect.e10s.isin(True, False))\
                                      .cache()

    # return filtered dataframe
    return filteredPingsDF
