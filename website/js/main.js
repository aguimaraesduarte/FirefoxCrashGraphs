var global = {
  heroIndex: 0,
  // chart defaults for MG
  chart: {
    width: 400,
    height: 660,
    left: 0,
    right: 120,
    xax_count: 4
  },
  // used when updating the
  // hours and days histogram
  currentDate: null,
  charts: null,
  // all firefox releases stored
  // in metrics graphics supported
  // format
  allMarkers: null,
  // // min and max hours & days
  // // for histograms
  // days: {
  //   min: null,
  //   max: null
  // },
  // hours: {
  //   min: null,
  //   max: null
  // }
}

// partially applied functions for loading
// json data for histograms for hours & days
// between use
var getHoursFilePath = getFilePath.bind(this, "hours")
var getDaysFilePath = getFilePath.bind(this, "days")

global.currentDate = previousDate(moment())

var daysFile = getDaysFilePath(global.currentDate)
var hoursFile = getHoursFilePath(global.currentDate)

var firefoxReleasesPath = "https://product-details.mozilla.org/1.0/firefox.json"

// main json file for first graphs &
// release json files are loaded in
// series. TODO: change to async if
// needed
d3.queue()
.defer(d3.json, "JSON/fx_crashgraphs.json")
.defer(d3.json, firefoxReleasesPath)
.await(function(error, fx_crashgraphs, firefoxReleases){
  // releases are stored {1.0rc1: "2004-10-27", ...}
  // data needs to be in the form [{'date' : Date(), 'label' : Label()}]
  global.allMarkers = [];

  firefoxReleases = firefoxReleases.releases;

  // filter out for releases that only
  // occured this year. one year is
  // arbitrary
  var lastYearStr = moment().subtract(1, 'years').format('YYYY-MM-DD');

  for(var release in firefoxReleases){
    if(firefoxReleases[release].date >= lastYearStr){
      global.allMarkers.push({
        date: firefoxReleases[release].date,
        // store category to filter by
        // particular release later
        category: firefoxReleases[release].category,
        label: firefoxReleases[release].version,
      });
    }
  }

  global.allMarkers = MG.convert.date(global.allMarkers, "date");

  // filter out all of the release canidates
  var filteredMarkers = global.allMarkers.filter(filterCategory.bind(this, "major"));

  // // temporarily compute percentage of wau that crashed
  // for(var i = 0; i < fx_crashgraphs.length; i++){
  //   fx_crashgraphs[i].proportion_wau_crashes = (fx_crashgraphs[i].num_crashed / fx_crashgraphs[i].wau);
  //   fx_crashgraphs[i].proportion_first_time_crashes = (fx_crashgraphs[i].num_first_time_crash / fx_crashgraphs[i].num_crashed);
  //   fx_crashgraphs[i].proportion_multiple_crashes = (fx_crashgraphs[i].num_multiple_crash / fx_crashgraphs[i].num_crashed);
  //   fx_crashgraphs[i].proportion_new_profiles = (fx_crashgraphs[i].num_new_profiles / fx_crashgraphs[i].wau);
  //   fx_crashgraphs[i].proportion_new_crashes = (fx_crashgraphs[i].num_crashed_new_profiles / fx_crashgraphs[i].num_new_profiles);
  //   fx_crashgraphs[i].proportion_e10s_enabled = (fx_crashgraphs[i].num_e10s_enabled / fx_crashgraphs[i].num_crashed);
  //   fx_crashgraphs[i].proportion_e10s_disabled = (fx_crashgraphs[i].num_e10s_disabled / fx_crashgraphs[i].num_crashed);
  // }

  fx_crashgraphs = MG.convert.date(fx_crashgraphs, "date");

  // custom chart properties that override
  // common properties initialized below
  var customChartProperties = [
    {
      title: "Crash Rates (per user, per 1,000h)",
      target: "#crash-rates",
      y_accessor: [//"crash_rate_main", "crash_rate_content", "crash_rate_plugin"],
                   "crash_rate_main_avg_by_user", "crash_rate_content_avg_by_user", "crash_rate_plugin_avg_by_user"],
      legend: ["crash_rate_main", "crash_rate_content", "crash_rate_plugin"],//,
               //"crash_rate_main_avg_by_user", "crash_rate_content_avg_by_user", "crash_rate_plugin_avg_by_user"],
      description: '<ul><li><kbd>crash_rate_main</kbd>: crashes experienced due to a <b>browser crash</b> (full crash) per user per thousand hours.\
        <br>(note: we use <i><a href="https://github.com/mozilla/telemetry-batch-view/blob/master/src/main/scala/com/mozilla/telemetry/views/MainSummaryView.scala#L666" target="_blank">crash_submit_success_main</a></i>)</li>\
        <li><kbd>crash_rate_content</kbd>: crashes experienced due to a <b>content crash</b> (tab crash) per user per thousand hours.\
        <br>(note: we use <i><a href="https://github.com/mozilla/telemetry-batch-view/blob/master/src/main/scala/com/mozilla/telemetry/views/MainSummaryView.scala#L661" target="_blank">crashes_detected_content</a></i>)</li>\
        <li><kbd>crash_rate_plugin</kbd>: crashes experienced due to a <b>plugin</b> per user per thousand hours.\
        <br>(note: we sum <i><a href="https://github.com/mozilla/telemetry-batch-view/blob/master/src/main/scala/com/mozilla/telemetry/views/MainSummaryView.scala#L660" target="_blank">crashes_detected_plugin</a></i> and <i><a href="https://github.com/mozilla/telemetry-batch-view/blob/master/src/main/scala/com/mozilla/telemetry/views/MainSummaryView.scala#L662" target="_blank">crashes_detected_gmplugin</a></i> as a single type of plugin crash)</li></ul>'
    },
    {
      title: "Percentage of Weekly Active Users that Crashed",
      target: "#percentage-crashed",
      y_accessor: ["proportion_wau_crashes"],
      legend: ["wau_crashes"],
      format: "percentage",
      aggregate_rollover: false,
      description: '<ul><li><kbd>wau_crashes</kbd>: out of all weekly active users, how many experienced a crash that week?</li></ul>'
    },
    {
      title: "Percentage of First Crashes Recorded",
      target: "#first-crashes",
      y_accessor: ["proportion_first_time_crashes", "proportion_multiple_crashes"],
      legend: ["first_time_crashes", "multiple_crashes"],
      format: "percentage",
      description: '<ul><li><kbd>multiple_crashes</kbd>: out of all users that crashed during the week, how many have had a prior crash in their history?</li>\
      <li><kbd>first_time_crashes</kbd>: out of all users that crashed during the week, for how many was it their first crash?</li></ul>'
    },
    {
      title: "Percentage of e10s Adoption",
      target: "#e10s",
      y_accessor: ["proportion_e10s_enabled", "proportion_e10s_disabled"],
      format: "percentage",
      legend: ["e10s_enabled", "e10s_disabled"],
      description: '<ul><li><kbd>e10s_enabled</kbd>: out of all weekly active users that crashed, how many had e10s enabled?</li>\
      <li><kbd>e10s_disabled</kbd>: out of all weekly active users that crashed, how many had e10s disabled?</li></ul>'
    },
    {
      title: "Crash Rates (per e10s status)",
      target: "#crash-rates-e10s",
      y_accessor: ["crash_rate_content_avg_by_user_and_e10s_enabled", "crash_rate_content_avg_by_user_and_e10s_disabled"],
      legend:["crash_rate_content_e10s_enabled", "crash_rate_content_e10s_disabled"],
      description: 'Variables are the same as in the first crash graph, but selecting only users that had e10s enabled/disabled that week.'
    },
    {
      title: "Percentage of New Profiles",
      target: "#new-profiles",
      y_accessor: ["proportion_new_profiles"],
      format: "percentage",
      legend: ["new_profiles"],
      aggregate_rollover: false,
      description: '<ul><li><kbd>new_profiles</kbd>: out of all weekly active users, how many created their profile that week?</li></ul>'
    },
    {
      title: "Percentage of New Profiles that Crashed",
      target: "#percentage-new-crashed",
      y_accessor: ["proportion_new_crashes"],
      legend: ["percentage_new_crashes"],
      format: "percentage",
      aggregate_rollover: false,
      description: '<ul><li><kbd>percentage_new_crashes</kbd>: out of all the new profiles created that week, how many experienced a crash?</li></ul>'
    },
    // {
    //   title: "Percentage of Last Crashes Recorded",
    //   target: "#last_crashes",
    //   y_accessor: ["prop_last_crash_main_tot", "prop_last_crash_content_tot", "prop_last_crash_plugin_tot",
    //                "prop_last_crash_main_mult", "prop_last_crash_content_mult", "prop_last_crash_plugin_mult",
    //                "prop_last_crash_main_first", "prop_last_crash_content_first", "prop_last_crash_plugin_first"],
    //   legend: ["prop_last_crash_main_tot", "prop_last_crash_content_tot", "prop_last_crash_plugin_tot",
    //            "prop_last_crash_main_mult", "prop_last_crash_content_mult", "prop_last_crash_plugin_mult",
    //            "prop_last_crash_main_first", "prop_last_crash_content_first", "prop_last_crash_plugin_first"],
    //   format: "percentage"
    // },
    {
      title: "Hours Between Crashes",
      target: "#hours_between_crashes",
      y_accessor: ["median_hours_between_crashes", "geom_hours_between_crashes"],
      legend: ["median_hours_between_crashes", "geom_mean_hours_between_crashes"],
      description: '<ul><li><kbd>median_hours_between_crashes</kbd>: median number of active hours between the two most recent crashes for users who have had multiple crashes.</li>\
      <li><kbd>geom_mean_hours_between_crashes</kbd>: geometric mean of active hours between the two most recent crashes for users who have had multiple crashes.</li></ul>'
    },
    // {
    //   title: "Days Between Crashes",
    //   target: "#days_between_crashes",
    //   y_accessor: ["median_days_between_crashes", "geom_days_between_crashes"],
    //   legend: ["median_days_between_crashes", "geom_mean_days_between_crashes"],
    //   description: '<ul><li><kbd>median_days_between_crashes</kbd>: median number of days between the two most recent crashes for users who have had multiple crashes.</li>\
    //   <li><kbd>geom_mean_days_between_crashes</kbd>: geometric mean of days between the two most recent crashes for users who have had multiple crashes.</li></ul>'
    // }
  ]

  // store common properties to be
  // overwritten by customProperties
  var commonChartProperties = []
  for(var i = 0; i < customChartProperties.length; i++){
    commonChartProperties.push({
      data: fx_crashgraphs,
      animate_on_load: true,
      width: global.chart.width,
      height: 300,
      xax_count: global.chart.xax_count,
      right: global.chart.right,
      full_width: true,
      x_accesor: "date",
      markers: filteredMarkers,
      aggregate_rollover: true
    })
  }

  // merge the custom properties & common properties
  function mapCharts(tuple){
    return Object.assign(tuple[0], tuple[1]);
  }
  global.charts = _.zip(commonChartProperties, customChartProperties).map(mapCharts)

  // draw each of the charts
  global.charts.forEach(function(chart){ MG.data_graphic(chart) });

  // bind click events to the histogram
  // buttons to fetch new data & update
  // both of the histograms
  d3.select('.hero-left')
  .on('click', function(){
    global.currentDate = previousDate(global.currentDate);

    var daysFile = getDaysFilePath(global.currentDate);
    var hoursFile = getHoursFilePath(global.currentDate);

    updateDaysAndHours(daysFile, hoursFile);
  });

  d3.select('.hero-right')
  .on('click', function(){
    global.currentDate = nextDate(global.currentDate);

    var daysFile = getDaysFilePath(global.currentDate);
    var hoursFile = getHoursFilePath(global.currentDate);

    updateDaysAndHours(daysFile, hoursFile);
  });
})

// initial draw of days & hours histograms
updateDaysAndHours(daysFile, hoursFile)

// called if there is no json for the
// corresponding address created
function createMissingDataChart(target){
  MG.data_graphic({
    title: "Missing Data",
    error: "Data is not available for the time period selected!",
    chart_type: "missing-data",
    missing_text: "Data is not available for the time period selected!",
    width: global.chart.width,
    height: 300,
    xax_count: global.chart.xax_count,
    right: global.chart.right,
    target: target,
    animate_on_load: false,
    full_width: true
  });
}

function updateDaysAndHours(daysFile, hoursFile){
  // TODO: Histograms don't have commonChartProperties
  // like the other charts did. This could be updated,
  // however it is low priority.
  var dates = daysFile.replace(".json", "")
    .split("-")
    .slice(1, 3)

  var dateStartParsed = dates[0].substr(0, 4) + "-" + dates[0].substr(4, 2) + "-" + dates[0].substr(6, 2)
  var dateEndParsed = dates[1].substr(0, 4) + "-" + dates[1].substr(4, 2) + "-" + dates[1].substr(6, 2)

  var dateStr = dateStartParsed + " : " + dateEndParsed
  d3.select(".formatted-date").text(dateStr)

  d3.queue()
  .defer(d3.json, daysFile)
  .defer(d3.json, hoursFile)
  .await(function(error, fx_crashgraphs_days, fx_crashgraphs_hours){
    var target = "#fx_crashgraphs_days";
    if(fx_crashgraphs_days){
      // TODO: replace min & max hard coded values w/ dynamic ones?
      // if(global.days.min == null){
      //   global.days.min = fx_crashgraphs_days.reduce(function(a, b){
      //     return a < b.days ? a : b.days
      //   }, Number.MAX_VALUE)
      //   global.days.max = fx_crashgraphs_days.reduce(function(a, b){
      //     return a > b.days ? a : b.days
      //   }, Number.MIN_VALUE)
      // }
      MG.data_graphic({
        title: "Count of Days Between Crashes per User",
        data: fx_crashgraphs_days,
        width: global.chart.width,
        height: 300,
        xax_count: global.chart.xax_count,
        right: global.chart.right,
        target: target,
        y_accessor: "count",
        x_accessor: "days",
        min_x: 0,
        max_x: 30,
        min_y: 0,
        max_y: 35000,
        transition_on_update: false,
        full_width: true
      });
    } else {
      createMissingDataChart(target);
    }

    var target = "#fx_crashgraphs_hours";
    if(fx_crashgraphs_hours){
      // if(global.hours.min == null){
      //   global.hours.min = fx_crashgraphs_hours.reduce(function(a, b){
      //     return a < b.hours ? a : b.hours
      //   }, Number.MAX_VALUE)
      //   global.hours.max = fx_crashgraphs_hours.reduce(function(a, b){
      //     return a > b.hours ? a : b.hours
      //   }, Number.MIN_VALUE)
      // }
      MG.data_graphic({
        title: "Count of Hours Between Crashes per User",
        data: fx_crashgraphs_hours,
        width: global.chart.width,
        height: 300,
        xax_count: global.chart.xax_count,
        right: global.chart.right,
        target: target,
        y_accessor: "count",
        x_accessor: "hours",
        min_x: 0,
        max_x: 30,
        min_y: 0,
        max_y: 35000,
        transition_on_update: false,
        full_width: true
      });
    } else {
      createMissingDataChart(target);
    }
  });
}

function updateMarkers(category){
  // update the markers for all of the timeseries charts (currently)
  var filteredMarkers = global.allMarkers.filter(filterCategory.bind(this, category));
  // update the charts
  global.charts = global.charts.map(function(chart){
    chart.markers = filteredMarkers;
    // not sure what this does
    delete chart.xax_format;
    return chart;
  });

  // apply to the charts
  global.charts.forEach(function(chart){ MG.data_graphic(chart) });
}

$('.split-by-controls button').click(function(){
  var category = $(this).data('category');
  updateMarkers(category)

  // change button state
  $(this).addClass('active').siblings().removeClass('active');
});



function updateCrashGraph(category){
  // change the yaccessors
  if (category=="main"){
    y_accessor = ["crash_rate_main_avg_by_user_and_e10s_enabled", "crash_rate_main_avg_by_user_and_e10s_disabled"],
    legend = ["crash_rate_main_e10s_enabled", "crash_rate_main_e10s_disabled"]
  } else if (category=="content"){
    y_accessor = ["crash_rate_content_avg_by_user_and_e10s_enabled", "crash_rate_content_avg_by_user_and_e10s_disabled"],
    legend = ["crash_rate_content_e10s_enabled", "crash_rate_content_e10s_disabled"]
  } else{
    y_accessor = ["crash_rate_plugin_avg_by_user_and_e10s_enabled", "crash_rate_plugin_avg_by_user_and_e10s_disabled"],
    legend = ["crash_rate_plugin_e10s_enabled", "crash_rate_plugin_e10s_disabled"]
  }
  // update the charts
  // TODO: get chart by id
  global.charts[4].y_accessor = y_accessor;
  global.charts[4].legend = legend;

  // not sure what this does
  delete global.charts[4].xax_format;

  MG.data_graphic(global.charts[4]);
}

$('.split-by-crash button').click(function(){
  var category = $(this).data('category');
  updateCrashGraph(category)

  // change button state
  $(this).addClass('active').siblings().removeClass('active');
});



// enable all tooltips
$(function(){
  $('[data-toggle="tooltip"]').tooltip()
});
