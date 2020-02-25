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




Discoveries
filename format is pageviews-YYYYMMDD-HH0000.gz
pageviews data format is <domain code> <page title> <count views> <total response size>
blacklist data format is <domain code> <page title>

Questions

1. Which sub-domains are there? Bullet point 3 says to aggregate to "unique domain". Is "unique domain" the same as "sub domain"? Are both of these equal to the first column of the pageviews file? If that is the case, then are we simply aggregating by the first column and returning the top 25 pages per domain code?
2. We only look at the pageview files, not the projectview files right
3. Do we include mobile as the same domain code? 
	e.g. do https://en.wikipedia.org/wiki/(You_Drive_Me)_Crazy_Tour and 
		https://en.m.wikipedia.org/wiki/(You_Drive_Me)_Crazy_Tour
		represented by 
		en (You_Drive_Me)_Crazy_Tour
		en.m (You_Drive_Me)_Crazy_Tour
		point to the same "domain"?
4. Bullet point 5, "only run if necessary" does that mean do not re-compute if a date+hour has already been computed/saved in the past?
5. Am I building the data pipeline/application or just describing how we would build it
6. Am I building the application as something that runs on AWS or runs locally? or is that up to me


More questions
1. Instead of strictly cutting off top 25 for each domain, pages tied for last place will be returned regardless of overrun. For example, if 3 pages are tied for 24th place, all 3 will be returned (thus 26 results). Is this okay?

2. I'm noticing that the page titles in the blacklist have different encodings than the page titles from the raw pageviews file.

for example:
The german page for Centipede appears as "de Hundertfüßer 2 0" in the pageviews file and "de Hundertf%C3%BC%C3%9Fer" in the blacklist file. 

I can fix this case with a simple url decoding. However, there are cases where straight url decoding will not yield the equivalent value. 

For example, the german page for Hitzacker appears as "de Hitzacker_(Elbe) 1 0" in the pageviews file and "de Hitzacker%20(Elbe)" in the blacklist file. Normally %20 decodes to the empty space character " " instead of underscore.

Should I be decoding the blacklist file before removing equivalent page titles? Should I treat %20 as a bug in the blacklist file and decode them to underscore instead of space, or leave as is?




[Assumptions]
1. We are treating mobile and web as separate domains. For example the web version of english page for Tokyo, "en Tokyo", will be treated separately from the mobile version of the same page, "en.m Tokyo".
