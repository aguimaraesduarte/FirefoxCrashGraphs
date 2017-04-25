# Firefox Crash Graphs
[Firefox Crash Graphs](https://people-mozilla.org/~sguha/mozilla/crashgraphs/) is a daily (weekdays) report of crash analysis on a representative 1% sample of the population from Firefox's release channel on desktop. Main, content, and plugin crashes are collected and analyzed for this sample of the population and provide an accurate representation of the state of Firefox experience for desktop users and can be used by developers to improve it.

# How is the report created?
The data for this report comes from Firefox’s built-in Telemetry data system. Firefox automatically collects information about crashes and sends this to Mozilla roughly daily, unless users disable this collection. This raw data is processed to remove corrupted or inaccurate entries and is then aggregated. This aggregation anonymizes the data, removing indicators that might be used to identify individual users.

At the end of the process, the aggregated, anonymized data is exported to a public JSON file and published [here](https://people-mozilla.org/~sguha/mozilla/crashgraphs/).

# Front-end Setup
In order to run the website locally the following commands must be run (`http-server` is a `node.js` dependency of this project). Installation of this dependency are well documented and found online. Bower can be used with this project, but a CDN is currently used.

```
$ cd website
$ http-server .
```

# Running the files
In order to run the crash graphs analysis, the first step is SCP'ing previous json files to the cluster (in the same directory as the python files). These are in the format `fx_crashgraphs-YYYYMMDD-YYYYMMDD.json` and available from S3.
Once all the json files are available on the cluster, analysis is run using the command

```bash
unset PYSPARK_DRIVER_PYTHON
unset PYSPARK_DRIVER_PYTHON_OPTS
spark-submit main.py
```

The job will read data from main_summary, calculate all the metrics, and save them to two json files:

- `fx_crashgraphs-YYYYMMDD-YYYYMMDD.json`: a summary of metrics for the given week;
- `fx_crashgraphs_hours-YYYYMMDD-YYYYMMDD.json`: a counter for hours between last two crashes for the given week.

In addition, one final json file (`fx_crashgraphs.json`) is created as an aggregated summary of all `fx_crashgraphs-YYYYMMDD-YYYYMMDD.json` files.

The main script will try to find the last week that was analyzed and run the analysis for every 7-day period ending on Monday, Wednesday, Friday between that date and when the script is being run (automatic backfilling). If no previous files are found that match the required format, the script will backfill until September 1st, 2016. In such case, `2*n+1` `.json` files are created, where `n` is the number of seven day periods to analyze within the date range. There is one of each file with the range as `YYYYMMDD-YYYYMMDD`, and one `fx_crashgraphs.json` file with an aggregated summary of metrics from all `n` `fx_crashgraphs-YYYYMMDD-YYYYMMDD.json` files.

# TODO

- Add to Airflow to run after specific jobs

# About us

[Saptarshi Guha](https://github.com/saptarshiguha) — Senior Data/Applied Statistics Scientist

[Andre Duarte](https://github.com/aguimaraesduarte) — Data Analyst

[Connor Ameres](https://github.com/cameres) — Data Analyst
