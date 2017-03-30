( this is an excerpt from a long email thread found here
[https://mail.mozilla.org/pipermail/fhr-dev/2017-February/001180.html]).


Some time back, in the end of 2016, I had an email conversation with
bsmedberg, harald kirshner, and john jensen pushing for

1) profile based measures e.g. what % of profiles crashed? How much
of crashes is contributed by what percent of profiles? I have not found
an answer to this on any mozilla dashboard

2) A different way to compute rates. Consider a each profiles Firefox
stability as a time series process where events are crashes. Compute the
crash rate for each profile and then average that. This has an easy
interpretation: an average crash rate of say 10 per profile per 1000
hours is the a value you'd see of a typical profile. This is different
from the rates typically plotted in say [^1] where the rate is total
crashes by total hours and has the effect of weighting users crash rates
by their usage. 

3) New user crash rates. How is it different from existing users?

So the focus of this dashboard is to provide an answer (1) with
everything else providing a context to (1) (and of course recomputing
rates differently)

I worked with our two interns Andre and Connor to get this
*proof-of-concept* of the ground to

4) gather feedback
5) generate discussion

It is not our primary goal to make this a production dashboard. That
said it looks good, because well, a spoonful of sugar makes the medicine
go down.

If we decide to absorb some measures (*e.g. % of profiles crashing) into
[1^] that will be great.

Also this very much a 'runnable' proof of concept. That is running it is a
almost a click of a button. So this can keep on  being updated and
people can eyeball it to see if the graphs are helping inform decisions.


The code is public and can be found here:
https://github.com/aguimaraesduarte/FirefoxCrashGraphs


> I don't quite understand "Percentage of First Crashes Recorded". How
> far back is "ever" in this? e.g. if somebody crashed two years ago and
> then again this week, how would that show up? This feels like the kind
> of graph which has unintentional consequences: people using the
> browser more will probably crash more and make the line go up, even
> though that's not necessarily an indication of a problem.

[MTBF and Hours] For a given profile, we take their entire history
present in main_summary.

How we compute time between crashes:

- for profiles with more than 1 crash
- compute time between this crash and last crash. If last crash occured
on the same day, then the time between is 0 else it is the sum of
session length between this crash and last crash
- average this hours between across profiles.

for profiles with only crash (an no other recorded crash in the history)
we decided to not make any assumptions. One could argue their last crash
is *at least* the sum of the previous session lengths. Instead we kept
these profiles separate and recorded the % of such profiles.

The role of this graph is not to indicate a problem. it is a diagnostic
in that it tells the viewer that the time between crashes has been
computed based on ~90% of the crashing profiles.




> "Percentage of New Profiles that Crashed" is one of the key baselines
> I was looking for, and I'm super-excited that we finally have it! I
> have some questions about the method. You are aggregating this by
> calendar week; does this mean both "users who are new within this
> week" and "users who crashed within this week"? Here's my concern:


How we run this: this is run on M,W and F for the calendar week ending
on M, W, and F respectively.



> User starts using Firefox on Friday.  User has a crash on Tuesday.
> According to my expected definition, this would be a crash in the
> user's first week and so would count against this graph. But if you're
> just aggregating by week, I'm not sure whether this is counted.

If i run this dashboard on M, this user will contribute to % of New
Profiles graph but not to the % of New Profiles that Crashed for her browser
hasn't crashed as yet.

If i run this on W, then this profile is a new user and has crashed and
will contribute. So this profile will be counted.

When i run this on F, this profile will not be considered a new user any
more. This graph is a % of new profiles (created in the week up to thdat
date) *and* crashed in that week



> "Hours Between Crashes" seems like a typical MTBF chart but I'm
> confused because the graph limits itself to users who have crashed
> more than once. What is the timeframe over which we're measuring these
> things? e.g. if there are users who never crash, or only crash once a
> year, wouldn't we want to include that in this kind of chart? Or how
> should I read this chart as a manager. We want to end up with more
> users never crashing, which seems like it would then make this chart
> appear worse instead of better.

See [MTBF and Hours] above.



> The "Count of Hours Between Crashes per User" graph is scary! Is this
> really per *user* or per *crash*. For example:

> User A crashes has never crashed before, and crashes 3 times in one
> hour. Does this show up as one crash with infinite uptime and two
> crashes with 0-hour uptime?  How does a user show up who didn't crash
> at all this week?

This user shows up as a crash with 0 time between crashes.

For methodology see [MTBF and Hours] above. That is

- for profiles that crashed this week *and* have one more crash in their entire history
- compute hours between crashes.
  - if on the day of their most recent crash they crashed 2+ times,
    then the time between crashes is 0
  - if on the day of their most recent crash, they crashed once, then
    the time between crashes is sum of session lengths till the crash before that
- average this across profiles (we used median and geometric mean to protect against outliers)

this average is then the truncated for average hours between crashes per
user. Note i use the word  'truncated' since it isn't *exactly* the hours
between two crashes - see [Hours Between Crashes] below.

- From figure "Percentage of Weekly Active Users that Crashed" we know
  how many profiles we are talking about
- from "Percentage of First Crashes Recorded" we have an idea of how
  many profiles we have dropped (because they don't have a prior crash)



> Also, how much resolution do these datasets have? e.g. if somebody has
> 2 content crashes per day, they would be recorded in one subsession
> (main ping), and I don't think we currently have internal timing data
> to distinguish those. Would those be counted as happening in the same
> hour, even though they could be anywhere from 0-23 hours apart? Do you
> think that affects the overall quality of this data? There is a
> similar question about the per-hour distribution graph below.

[Hours Between Crashes] You are right - but it is consistent across time. 
We aggregate the data to the daily level:

client_id, submission_date, total main_crashes, total_plugin_crashs,...,total_crashes

If on the latest day in the week, total_crashes >= 2 , the time between crashes
is 0 (even if it occurred 10 hours apart).

if on the latest day in the week, total_crashes == 1 the time between
crashes is the sum of session length from that day to the crash before
that.

Yes, we lose granularity. But it is a representative, consistent view of
hours between crashes per profile. Even if we measured the exact hours
between two crashes, the cognivitve rule stays the same: a decreasing
line ==> crashes occur more often.

Moreover, the two figures, given the way we have computed

"Crash Rates (per user, per 1,000h)"

and

"Hours Between Crashes"

are related: if the former goes  up, the latter goes down. You can think
of a Poisson process: if the rate of occurrence goes up, the time between
occurrences decreases. 


Thanks so much for the great questions and feedback. This is why we sent
it to FHR-dev.

Regards
Saptarshi


[^1]: https://telemetry.mozilla.org/crashes/
