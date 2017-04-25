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
           sum(cdc - sk) as cdc,
           sum(cdgmp + cdp) as cdpgmp
    FROM {table}
    GROUP BY cid, sd, e10s
    """.format(table=filteredDF_str)

    aggregateDF = sqlContext.sql(query).cache()
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
    WHERE sd BETWEEN '{start}' AND '{end}'
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
    WHERE sd BETWEEN '{start}' AND '{end}'
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
    WHERE sd BETWEEN '{start}' AND '{end}'
    AND ssl>0
    """.format(table=aggregateDF_str,
               start=start_date_str,
               end=end_date_str)

    wau7 = sqlContext.sql(query).collect()

    return wau7[0].wau7


def get_num_crashed(aggregateDF_str, start_date_str, end_date_str, l_crashes=[1,2]):
    """
    This function calculates and returns the total number of profiles that crashed n+ times between the two date arguments,
    where n is specified in the arguments.

    @params:
        aggregateDF_str: [string] name of the dataframe returned by aggregate_by_client_date_e10s(...)
        start_date_str: [string] start date for analysis
        end_date_str: [string] end date for analysis
        l_crashes: [list] list of crashes to get. i.e., for 1+ and 2+ crashes, l_crashes=[1,2] (default)

    @return:
        [list] a list of each number of profiles that crashed n+ times accorting to the input parameter.
    """

    tup_crashes = sorted(set([(1,1), (2,2)] + [(i,i) for i in l_crashes])) # always have at least 1 and 2

    case_statement = "CASE WHEN total_crashes >= %d THEN 1 ELSE 0 END AS case%d"
    case_statements = [case_statement%(i) for i in tup_crashes]
    str_case_statements = ", ".join(case_statements)

    sum_statement = "SUM(case%d) AS total_crashes%d"
    sum_statements = [sum_statement%(i) for i in tup_crashes]
    str_sum_statements = ", ".join(sum_statements)

    query = """
    SELECT {sums}
    FROM
    (
        SELECT cid, {cases}
        FROM
        (
            SELECT cid, SUM(cssm + cdc) AS total_crashes
            FROM {table}
            WHERE sd BETWEEN '{start}' AND '{end}'
            AND ssl>0
            GROUP BY cid
        )
    )
    """.format(table=aggregateDF_str,
               start=start_date_str,
               end=end_date_str,
               sums=str_sum_statements,
               cases=str_case_statements)

    crashed_profiles = sqlContext.sql(query)

    number_of_crashed_profiles = crashed_profiles.collect()

    return [num_crashed for num_crashed in number_of_crashed_profiles[0]]


def get_num_new_profiles(aggregateDF_str, start_date_str, end_date_str):
    """
    This function calculates and returns the number of new profiles (up to 2 weeks of activity) that
    were active between the two date arguments.

    @params:
        aggregateDF_str: [string] name of the dataframe returned by aggregate_by_client_date_e10s(...)
        start_date_str: [string] start date for analysis
        end_date_str: [string] end date for analysis
    """

    query = """
    SELECT count(distinct cid) as new_profiles
    FROM {table}
    WHERE sd BETWEEN '{start_sd}' AND '{end_sd}'
    AND pcd BETWEEN {start_pcd} AND {end_pcd}
    AND ssl>0
    """.format(table=aggregateDF_str,
               start_sd=start_date_str,
               end_sd=end_date_str,
               start_pcd=date2int(str2date(start_date_str))-7,
               end_pcd=date2int(str2date(end_date_str)))

    new_profiles = sqlContext.sql(query).collect()

    return new_profiles[0].new_profiles


def get_num_new_profiles_crashed(aggregateDF_str, start_date_str, end_date_str, l_crashes=[1,2]):
    """
    This function calculates and returns the total number of new profiles (up to 2 weeks of activity)
    that crashed n+ between the two date arguments, where n is specified by the arguments.

    @params:
        aggregateDF_str: [string] name of the dataframe returned by aggregate_by_client_date_e10s(...)
        start_date_str: [string] start date for analysis
        end_date_str: [string] end date for analysis
        l_crashes: [list] list of crashes to get. i.e., for 1+ and 2+ crashes, l_crashes=[1,2] (default)

    @return:
        [list] a list of each number of new profiles that crashed n+ times accorting to the input parameter.
    """

    tup_crashes = sorted(set([(1,1), (2,2)] + [(i,i) for i in l_crashes])) # always have at least 1 and 2

    case_statement = "CASE WHEN total_crashes >= %d THEN 1 ELSE 0 END AS case%d"
    case_statements = [case_statement%(i) for i in tup_crashes]
    str_case_statements = ", ".join(case_statements)

    sum_statement = "SUM(case%d) AS total_crashes%d"
    sum_statements = [sum_statement%(i) for i in tup_crashes]
    str_sum_statements = ", ".join(sum_statements)

    query = """
    SELECT {sums}
    FROM
    (
        SELECT cid, {cases}
        FROM
        (
            SELECT cid, SUM(cssm + cdc) AS total_crashes
            FROM {table}
            WHERE sd BETWEEN '{start}' AND '{end}'
            AND pcd BETWEEN {start_pcd} AND {end_pcd}
            AND ssl>0
            GROUP BY cid
        )
    )
    """.format(table=aggregateDF_str,
               start=start_date_str,
               end=end_date_str,
               start_pcd=date2int(str2date(start_date_str))-7,
               end_pcd=date2int(str2date(end_date_str)),
               sums=str_sum_statements,
               cases=str_case_statements)

    crashed_profiles = sqlContext.sql(query)

    number_of_crashed_profiles = crashed_profiles.collect()

    return [num_crashed for num_crashed in number_of_crashed_profiles[0]]


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
               SUM(cssm + cdc) as total_crashes
        FROM {table}
        WHERE sd BETWEEN '{start}' AND '{end}'
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
    WHERE sd BETWEEN '{start}' AND '{end}'
    AND cssm + cdc > 0) AS LHS

    LEFT JOIN

    {rhs}

    USING(cid)
    """.format(lhs=aggregateDF_str,
               start=start_date_str,
               end=end_date_str,
               rhs=aggregateDF_str)

    aggregate_crashed_clients_in_week = sqlContext.sql(query)

    return aggregate_crashed_clients_in_week


def aggregate_new_users(aggregateDF_str, start_date_str, end_date_str):
    """
    This function creates and returns a subset of the aggregate table containing only the rows
    concerning profiles that were created 3 weeks prior to end_date_str.

    @params:
        aggregateDF_str: [string] name of the dataframe returned by aggregate_by_client_date_e10s(...)
        start_date_str: [string] start date for analysis (-19)
        end_date_str: [string] end date for analysis (-13)
    """

    query = """
    SELECT *
    FROM

    (SELECT distinct cid
    FROM {lhs}
    WHERE pcd BETWEEN {start} AND {end}
    ) AS LHS

    LEFT JOIN

    {rhs}

    USING(cid)
    """.format(lhs=aggregateDF_str,
               start=date2int(str2date(end_date_str)) - 19,
               end=date2int(str2date(end_date_str)) - 13,
               rhs=aggregateDF_str)

    aggregate_new_clients_in_week = sqlContext.sql(query)

    return aggregate_new_clients_in_week


def make_longitudinal(agg_subset):
    """
    This function creates and returns a longitudinal dataframe from the aggregate dataframe grouped by client_id (cid).
    Each Row from this dataframe contains the sequential information (lists) for:
        - subsession_length (ssl),
        - submission_date (sd),
        - crash_submit_success_main (cssm),
        - crash_detected_content (cdc),
        - crash_detected_plugin + crash_detected_gmplugin (cdpgmp)
    for each cid and the profile_creation_date (pcd)

    @params:
        agg_subset: [dataframe] dataframe returned by aggregate_subset(...)
    """

    longitudinal = agg_subset.groupBy(agg_subset.cid)\
                             .agg({"pcd": "first",
                                   "sd": "collect_list",
                                   "ssl": "collect_list",
                                   "cssm": "collect_list",
                                   "cdc": "collect_list",
                                   "cdpgmp": "collect_list"})\
                             .withColumnRenamed("first(pcd)", "pcd")\
                             .withColumnRenamed("collect_list(sd)", "sd")\
                             .withColumnRenamed("collect_list(ssl)", "ssl")\
                             .withColumnRenamed("collect_list(cssm)", "cssm")\
                             .withColumnRenamed("collect_list(cdc)", "cdc")\
                             .withColumnRenamed("collect_list(cdpgmp)", "cdpgmp")
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
                                          "total_ssl_between_crashes"])

    if filter_str != "":
        crash_statistics_pd = crash_statistics_pd.filter(filter_str)

    if select_cols != []:
        crash_statistics_pd = crash_statistics_pd.select(select_cols)

    crash_statistics_pd = crash_statistics_pd.toPandas()

    return crash_statistics_pd


def mapCrashes(row):
    """
    Applied to an RDD, this mapping function returns a tuple of elements for each Row of the dataframe:
        - has_multiple_crashes: [True/False] whether the client_id has more than a single crash in its history
        - total_ssl_between_crashes: [float/None] number of hours between the two most recent crashes

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
        - if there are multiple crashes on that day, the time between crashes is 0 hours
        - if there is only one crash on that day
            - find the day of the next most recent crash
                - if there is one, then get the sum of all subsession lengths for that period
                - if there is no previous crash in the history for that profile, we cannot measure
                    time between crashes
    """

    # return the sum of all crashes for a given submission date index
    def sumCrashes(row, index):
        return sum([row.cssm[index], row.cdc[index]])

    # return boolean whether there was a crash for a given submission date index
    def isCrash(row, index):
        return sumCrashes(row, index) > 0

    # sort row lists by submission_date (most recent first)
    def sort_row(row):
        zipped = sorted(zip(row.sd, row.cdc, row.cssm, row.cdpgmp, row.ssl), reverse=True)
        sd, cdc, cssm, cdpgmp, ssl = zip(*zipped)
        return Row(cid=row.cid,
                   sd=list(sd),
                   ssl=list(ssl),
                   cdc=list(cdc),
                   cssm=list(cssm),
                   cdpgmp=list(cdpgmp))

    first_crash = None                   # submission date index of first (latest) crash
    next_crash = None                    # submission date index of second (second latest) crash

    has_multiple_crashes = False         # whether user has a previous crash
    total_ssl_between_crashes = None     # total subsession length between crashes

    # sort the row by submission_date
    sorted_row = sort_row(row)

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
            total_ssl_between_crashes) # in hours


def mapCrashes_new(row):
    """
    Applied to an RDD, this mapping function returns a tuple (group, 1) for each Row of the dataframe.
    These can be counted by key to get aggregate numbers per group.

    @params:
        row: [Row] a row from a longitudinal RDD that includes:
            - pcd: profile_creation_date
            - sd: submission_date
            - ssl: subsession_length
            - cssm: crash_submit_success_main
            - cdc: crash_detected_main
            - cdpgmp: crash_detected_plugin + crash_detected_gmplugin

    @logic:
        For each profile, determine the number of crashes in the first two weeks of activity since pcd.
        Depending on how many crashes occurred, user is assigned to one of two groups:
            - group 0: no crashes within 14 days of pcd
            - group 1: 1 crash within 14 days of pcd
    """

    # return the sum of all crashes for a given submission date index
    def sumCrashes(row, index):
        return sum([row.cssm[index], row.cdc[index]])

    # sort row lists by submission_date (chronological)
    def sort_row(row):
        zipped = sorted(zip(row.sd, row.cdc, row.cssm, row.cdpgmp, row.ssl), reverse=False)
        sd, cdc, cssm, cdpgmp, ssl = zip(*zipped)
        return Row(cid=row.cid,
                   pcd=row.pcd,
                   sd=list(sd),
                   ssl=list(ssl),
                   cdc=list(cdc),
                   cssm=list(cssm),
                   cdpgmp=list(cdpgmp))

    # sort the row by submission_date
    sorted_row = sort_row(row)

    # get dates for first two weeks of activity
    start_date_int = sorted_row.pcd
    end_date_int = start_date_int + 13
    start_date_str = date2str(int2date(start_date_int))
    end_date_str = date2str(int2date(end_date_int))

    # iterate through all subsessions (brute force):
    tot_crashes = 0
    for index, submission_date in enumerate(sorted_row.sd):
        # if date is between what we want, the count number of crashes that day
        if submission_date <= end_date_str:
            tot_crashes += sumCrashes(sorted_row, index)
        # since the list is sorted, we can break as soon as we pass the end_date variable
        else:
            break

    # define groups
    group = 0
    if tot_crashes >= 1:
        group = 1

    return (group, 1)
