# dd2020

Wikipedia pageview data pipeline

Build a simple application that computes the top 25 pages on Wikipedia for each of the Wikipedia sub-domains:

Which sub-domains are there


1. Accept input parameters for the date and hour of data to analyze (default to current date/hour if not passed)
2. Download the page view counts from wikipedia for the given date/hour 
	at dumps.wikimedia.org/other/pageviews/YYYY/YYYY-MM/pageviews-YYYYMMDD-HH0000.gz
3. Eliminate any pages found in the blacklist. 
	Compute the top 25 articles for the given day and hour by total pageviews for each unique domain in the remaining data.
4. save results to file, either locally or on S3, sorted by domain and number of pageviews
5. only run the above steps if necessary, e.g. not if the work has already been done
6. be capable of being run for a range of dates and hours. each hour within the range should have its own result file.

For your solution explain:
1. What additional things would you want to operate this application in a production setting
2. What might change about your solution if this application needed to run automatically for each hour of the day
3. How would you test this application
4. How would you improve on the application design



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