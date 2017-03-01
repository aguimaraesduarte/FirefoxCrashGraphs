from pyspark.sql.functions import collect_list
from pyspark.sql import Row

from date_utils import *

sqlContext = None

def setup_transform(sqlC):
    global sqlContext
    sqlContext = sqlC

def aggregate_by_client_date_e10s(filteredDF_str):
    """
    This function creates and returns a table that is aggregated by client_id (cid), submission_date (sd), and e10s status (e10s).
    For each tuple (cid, sd, e10s), the following sums are aggregated:
        - subsession_length (ssl) -> total subsession_length for that (cid, sd)
        - crash_submit_success_main (cssm) -> total main crashes for that (cid, sd)
        - crash_detected_content (cdc) -> total content crashes for that (cid, sd)
        - crash_detected_plugin + crash_detected_gmplugin (cdpgmp) -> total plugin crashes for that (cid, sd)

    @params:
        filteredDF_str: [string] name of the dataframe returned by read_main_summary()
    """

    query = """
    SELECT cid,
           sd,
           e10s,
           first(pcd) as pcd,
           sum(ssl) as ssl,
           sum(cssm) as cssm,
           sum(cdc) as cdc,
           sum(cdgmp + cdp) as cdpgmp
    FROM {table}
    GROUP BY cid, sd, e10s
    """.format(table=filteredDF_str)

    aggregateDF = sqlContext.sql(query)
    return aggregateDF

def get_crash_rates_by_user(aggregateDF_str, start_date_str, end_date_str):
    """
    This function calculates and returns a tuple of three crash rates:
        - main crash`es per hour
        - content crashes per hour
        - plugin crashes per hour
    These rates are averages per user.

    @params:
        aggregateDF_str: [string] name of the dataframe returned by aggregate_by_client_date_e10s(...)
        start_date_str: [string] start date for analysis
        end_date_str: [string] end date for analysis

    **Potentially change the denominator to get rates per page load instead**
    """

    query = """
    SELECT AVG(mc) AS main_crashes_per_hour,
           AVG(cc) AS content_crashes_per_hour,
           AVG(pc) AS plugin_crashes_per_hour
    FROM
    (
    SELECT cid,
           SUM(cssm) / ((SUM(ssl) / 60 / 60)) AS mc,
           SUM(cdc) / ((SUM(ssl) / 60 / 60)) AS cc,
           SUM(cdpgmp) / ((SUM(ssl) / 60 / 60)) AS pc
    FROM {table}
    WHERE sd BETWEEN {start} AND {end}
    GROUP BY cid
    )
    """.format(table=aggregateDF_str,
               start=start_date_str,
               end=end_date_str)

    crash_rates = sqlContext.sql(query).collect()

    ret = (crash_rates[0].main_crashes_per_hour,
           crash_rates[0].content_crashes_per_hour,
           crash_rates[0].plugin_crashes_per_hour)

    return ret

def get_crash_rates_by_user_and_e10s(aggregateDF_str, start_date_str, end_date_str):
    """
    This function calculates and returns a tuple of three crash rates:
        - main crashes per hour
        - content crashes per hour
        - plugin crashes per hour
    These rates are averages per user per e10s status (enabled/disabled).

    @params:
        aggregateDF_str: [string] name of the dataframe returned by aggregate_by_client_date_e10s(...)
        start_date_str: [string] start date for analysis
        end_date_str: [string] end date for analysis

    **Potentially change the denominator to get rates per page load instead**
    """

    query = """
    SELECT e10s,
           AVG(mc) AS main_crashes_per_hour,
           AVG(cc) AS content_crashes_per_hour,
           AVG(pc) AS plugin_crashes_per_hour
    FROM
    (
    SELECT cid,
           e10s,
           SUM(cssm) / ((SUM(ssl) / 60 / 60)) AS mc,
           SUM(cdc) / ((SUM(ssl) / 60 / 60)) AS cc,
           SUM(cdpgmp) / ((SUM(ssl) / 60 / 60)) AS pc
    FROM {table}
    WHERE sd BETWEEN {start} AND {end}
    GROUP BY cid, e10s
    )
    GROUP BY e10s
    """.format(table=aggregateDF_str,
               start=start_date_str,
               end=end_date_str)

    crash_rates = sqlContext.sql(query).collect()

    crash_rates_e10s_enabled = [v for v in crash_rates if v.e10s==True]
    crash_rates_e10s_disabled = [v for v in crash_rates if v.e10s==False]

    ret = (crash_rates_e10s_enabled[0].main_crashes_per_hour,
           crash_rates_e10s_enabled[0].content_crashes_per_hour,
           crash_rates_e10s_enabled[0].plugin_crashes_per_hour,
           crash_rates_e10s_disabled[0].main_crashes_per_hour,
           crash_rates_e10s_disabled[0].content_crashes_per_hour,
           crash_rates_e10s_disabled[0].plugin_crashes_per_hour)

    return ret

def get_wau7(aggregateDF_str, start_date_str, end_date_str):
    """
    This function calculates and returns the weekly active users between the two date arguments.

    @params:
        aggregateDF_str: [string] name of the dataframe returned by aggregate_by_client_date_e10s(...)
        start_date_str: [string] start date for analysis
        end_date_str: [string] end date for analysis
    """

    query = """
    SELECT count(distinct cid) as wau7
    FROM {table}
    WHERE sd BETWEEN {start} AND {end}
    AND ssl>0
    """.format(table=aggregateDF_str,
               start=start_date_str,
               end=end_date_str)

    wau7 = sqlContext.sql(query).collect()

    return wau7[0].wau7

def get_num_crashed(aggregateDF_str, start_date_str, end_date_str):
    """
    This function calculates and returns the total number of profiles that crashed between the two date arguments.

    @params:
        aggregateDF_str: [string] name of the dataframe returned by aggregate_by_client_date_e10s(...)
        start_date_str: [string] start date for analysis
        end_date_str: [string] end date for analysis
    """

    query = """
    SELECT cid, SUM(cssm + cdc + cdpgmp) as total_crashes
    FROM {table}
    WHERE sd BETWEEN {start} AND {end}
    AND ssl>0
    GROUP BY cid
    HAVING total_crashes > 0
    """.format(table=aggregateDF_str,
               start=start_date_str,
               end=end_date_str)

    crashed_profiles = sqlContext.sql(query)

    number_of_crashed_profiles = crashed_profiles.count()

    return number_of_crashed_profiles

def get_num_new_profiles(aggregateDF_str, start_date_str, end_date_str):
    """
    This function calculates and returns the number of new profiles (created between the two date arguments) that
    were active between the two date arguments.

    @params:
        aggregateDF_str: [string] name of the dataframe returned by aggregate_by_client_date_e10s(...)
        start_date_str: [string] start date for analysis
        end_date_str: [string] end date for analysis
    """

    query = """
    SELECT count(distinct cid) as new_profiles
    FROM {table}
    WHERE sd BETWEEN {start_sd} AND {end_sd}
    AND pcd BETWEEN {start_pcd} AND {end_pcd}
    AND ssl>0
    """.format(table=aggregateDF_str,
               start_sd=start_date_str,
               end_sd=end_date_str,
               start_pcd=date2int(str2date(start_date_str)),
               end_pcd=date2int(str2date(end_date_str)))

    new_profiles = sqlContext.sql(query).collect()

    return new_profiles[0].new_profiles

def get_num_new_profiles_crashed(aggregateDF_str, start_date_str, end_date_str):
    """
    This function calculates and returns the total number of new profiles (created between the two date arguments)
    that crashed between the two date arguments.

    @params:
        aggregateDF_str: [string] name of the dataframe returned by aggregate_by_client_date_e10s(...)
        start_date_str: [string] start date for analysis
        end_date_str: [string] end date for analysis
    """

    query = """
    SELECT cid, SUM(cssm + cdc + cdpgmp) as total_crashes
    FROM {table}
    WHERE sd BETWEEN {start_sd} AND {end_sd}
    AND pcd BETWEEN {start_pcd} AND {end_pcd}
    AND ssl>0
    GROUP BY cid
    HAVING total_crashes > 0
    """.format(table=aggregateDF_str,
               start_sd=start_date_str,
               end_sd=end_date_str,
               start_pcd=date2int(str2date(start_date_str)),
               end_pcd=date2int(str2date(end_date_str)))

    crashed_new_profiles = sqlContext.sql(query)

    number_of_crashed_new_profiles = crashed_new_profiles.count()

    return number_of_crashed_new_profiles

def get_e10s_counts(aggregateDF_str, start_date_str, end_date_str):
    """
    This function calculates and returns the total number of profiles (created between the two date arguments)
    that have and do not have e10s enabled that crashed between the two date arguments.

    @params:
        aggregateDF_str: [string] name of the dataframe returned by aggregate_by_client_date_e10s(...)
        start_date_str: [string] start date for analysis
        end_date_str: [string] end date for analysis
    """

    query = """
        SELECT e10s,
               count(*) as cnt
        FROM
        (
        SELECT cid,
               e10s,
               SUM(cssm + cdc + cdpgmp) as total_crashes
        FROM {table}
        WHERE sd BETWEEN {start} AND {end}
        GROUP BY cid, e10s
        HAVING total_crashes > 0
        )
        GROUP BY e10s
        """.format(table=aggregateDF_str,
                   start=start_date_str,
                   end=end_date_str)

    cnts = sqlContext.sql(query).collect()

    num_e10s_enabled = [v.cnt for v in cnts if v.e10s==True][0]
    num_e10s_disabled = [v.cnt for v in cnts if v.e10s==False][0]

    return num_e10s_enabled, num_e10s_disabled

def aggregate_subset(aggregateDF_str, start_date_str, end_date_str):
    """
    This function creates and returns a subset of the aggregate table containing only the rows
    concerning profiles that crashed between the two date arguments.

    @params:
        aggregateDF_str: [string] name of the dataframe returned by aggregate_by_client_date_e10s(...)
        start_date_str: [string] start date for analysis
        end_date_str: [string] end date for analysis
    """

    query = """
    SELECT *
    FROM

    (SELECT distinct cid
    FROM {lhs}
    WHERE sd BETWEEN {start} AND {end}
    AND cssm + cdc + cdpgmp > 0) AS LHS

    LEFT JOIN

    {rhs}

    USING(cid)
    """.format(lhs=aggregateDF_str,
               start=start_date_str,
               end=end_date_str,
               rhs=aggregateDF_str)

    aggregate_crashed_clients_in_week = sqlContext.sql(query)

    return aggregate_crashed_clients_in_week

def make_longitudinal(agg_subset):
    """
    This function creates and returns a longitudinal dataframe from the aggregate dataframe grouped by client_id (cid).
    Each Row from this dataframe contains the sequential information (lists) for:
        - subsession_length (ssl),
        - submission_date (sd),
        - crash_submit_success_main (cssm),
        - crash_detected_content (cdc),
        - crash_detected_plugin + crash_detected_gmplugin (cdpgmp)
    for each cid.

    @params:
        agg_subset: [dataframe] dataframe returned by aggregate_subset(...)
    """

    longitudinal = agg_subset.groupBy(agg_subset.cid)\
                             .agg({"sd": "collect_list",
                                   "ssl": "collect_list",
                                   "cssm": "collect_list",
                                   "cdc": "collect_list",
                                   "cdpgmp": "collect_list"})\
                             .withColumnRenamed("collect_list(sd)","sd")\
                             .withColumnRenamed("collect_list(ssl)","ssl")\
                             .withColumnRenamed("collect_list(cssm)","cssm")\
                             .withColumnRenamed("collect_list(cdc)","cdc")\
                             .withColumnRenamed("collect_list(cdpgmp)","cdpgmp")
    return longitudinal

def RDD_to_pandas(mappedRDD, filter_str="", select_cols = []):
    """
    This function converts the rdd passed as an argument to a pandas dataframe.
    This is useful for creating graphs and getting summary statistics for the time between crashes.

    @params:
        mappedRDD: [rdd] RDD obtained using the map function mapCrashes(...)
        filter_str: [string] filter command. If empty, no filter
        select_cols: [list] select columns. If empy, no select
    """

    crash_statistics_pd = mappedRDD.toDF(["has_multiple_crashes",
                                          "total_ssl_between_crashes",
                                          "days_between_crashes",
                                          "has_main_crash",
                                          "has_content_crash",
                                          "has_plugin_crash",
                                          "last_crash_type"])

    if filter_str != "":
        crash_statistics_pd = crash_statistics_pd.filter(filter_str)

    if select_cols != []:
        crash_statistics_pd = crash_statistics_pd.select(select_cols)

    crash_statistics_pd = crash_statistics_pd.toPandas()

    return crash_statistics_pd

def getCountsLastCrashes(pd_df):
    """
    This function counts distinct values of last crashes for a pandas dataframe.
    Returns a tuple of three counts: for all users, users who crashes multiple times, and users who crashed for their first time.
    Single values for each crash type can then be accessed by its index or name.

    @params:
        pd_df: [pandas DF] pandas data frame to get the counts of
    """
    counts_tot = pd_df.last_crash_type.value_counts() / pd_df.shape[0]
    counts_mult = pd_df[pd_df.has_multiple_crashes==True].last_crash_type.value_counts() / pd_df[pd_df.has_multiple_crashes==True].shape[0]
    counts_first = pd_df[pd_df.has_multiple_crashes==False].last_crash_type.value_counts() / pd_df[pd_df.has_multiple_crashes==False].shape[0]

    return (counts_tot, counts_mult, counts_first)
