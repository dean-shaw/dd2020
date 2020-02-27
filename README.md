# dd2020
[Problem Statement
Wikipedia pageview data pipeline
Build a simple application that computes the top 25 pages on Wikipedia for each of the Wikipedia sub-domains:


[Assumptions]
1. Raw data is located online at "https://dumps.wikimedia.org/other/pageviews/{year}/{year}-{month}/pageviews-{year}{month}{day}-{hour}0000.gz"

2. pageviews data format is <domain code> <page title> <count views> <total response size>

3. blacklist data format is <domain code> <page title>

4. We are returning top 25 page titles, ranked by view count, per domain code

5. We are treating mobile and web as separate domains. For example the web version of english page for Tokyo, "en Tokyo", will be treated separately from the mobile version of the same page, "en.m Tokyo".

6. Instead of strictly cutting off top 25 for each domain, pages tied for last place will be returned regardless of overrun. For example, if 3 pages are tied for 24th place, all 3 will be returned (thus 26 results).

7. Page titles in the blacklist have different encodings than the page titles from the raw pageviews file.

for example:
The german page for Centipede appears as "de Hundertfüßer 2 0" in the pageviews file and "de Hundertf%C3%BC%C3%9Fer" in the blacklist file. 

I fixed this case with a simple url decoding. However, there are cases where straight url decoding will not yield the equivalent value. 

For example, the german page for Hitzacker appears as "de Hitzacker_(Elbe) 1 0" in the pageviews file and "de Hitzacker%20(Elbe)" in the blacklist file. Normally %20 decodes to the empty space character " " instead of underscore.

I treat this as a bug in the blacklist, therefore Hitzacker will not be filtered out.


[Installation]
1. Create a new python environment (via conda for example) and install the packages in requirements.txt


[Running Tests]
1. run unit tests by "python -m unittest WikiAggregator"


[Running Program]
1. run program by "python WikiAggregator.py --start <start timestamp> --end <end timestamp>"
	where timestamps are in the format YYYYMMDDHH
	start and end timestamps are inclusive
2. each run will produce two local artifacts,
	the raw csv downloaded from the data repo at pageviews-YYYYMMDD-HH.csv,
	the computed results at top25-YYYYMMDD-HH.csv


[Next Steps]
1. What additional things would you want to operate this application in a production setting

    CICD Pipeline:
    changes to python code automatically run unit tests and integration tests before uploading artifacts to S3.

    Core Data Pipeline:
    Cloudwatch trigger or API Gateway invocation -> Lambda -> EMR -> download data from web, write results to S3
    If we want faster jobs, we can have a permanently running EMR cluster and submit steps to it, instead of creating an EMR cluster every time.
    If we want even more control, we can replace the Cloudwatch trigger with our own orchestration component (e.g. Airflow).

    Monitoring, Alerting, Reporting, Failure:
    Monitor batch job successes and failures with Airflow or a custom Datadog metric.
    Add in automatic retries (up to a limit) for batch failures.
    Batch failures should alert Datadog/PagerDuty when all retries have been exhausted.
    Pipeline should easily be able to run in more than one AWS region. This will require making deployment scripts region-agnostic. We could run blue-green or hot-hot, depending on SLO requirements.

2. What might change about your solution if this application needed to run automatically for each hour of the day
    See answer 1

3. How would you test this application
    Unit testing should be expanded to assert values, not just testing completion.
    In addition to the included unit testing, an end-to-end integration test would be nice.
    The end-to-end integration test would run the aggregation logic on a static set of data, with expected results.

4. How would you improve on the application design
    The largest risk I see in the pipeline is a heavy dependency on outside data.
    I would introduce additional components to certify the robustness of the source data such as:
        cacheing of source data (in case data source is down)
        data quality checks for both source data and results data
        metadata repository, for cases when the source data changes schema
        event-driven alerting, if source data is down, the WikiAggregator should not even need to run.
        similarly, once WikiAggregator finishes, we should publish an event to trigger additional downstream jobs.

