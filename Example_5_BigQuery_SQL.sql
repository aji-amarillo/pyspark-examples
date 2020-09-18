#<--- BIGQUERY DOCUMENTATION @ https://support.google.com/analytics/answer/3437719?hl=en --->

##### 1. # AVERAGE SESSIONS GENERATED PER USER #####
# SOLUTION: 2.13 sessions/user

SELECT 
ROUND(SUM(ga.nr_sessions)/COUNT(DISTINCT ga.user_id),2) AS avg_sessions_per_user

FROM (
		--> SESSIONS GENERATED PER INDIVIDUAL USER
		SELECT
		fullvisitorid AS user_id,
		COUNT(DISTINCT visitId) AS nr_sessions
		--> a session has a unique ID, usually defined by time-bound criteria (e.g. any activity within 30 mins). A session can have multiple visits.

		FROM `dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export`

		GROUP BY 1
		ORDER BY 2 DESC
	) ga
;

##### 2. AVG. TIME TO REACH CONFIRMATION PAGE #####

# SOLUTION A: 5.1 mins
SELECT 
ROUND(AVG(A.time_difference_ss),2) AS avg_diff_seconds,
ROUND(AVG(A.time_difference_ss)/60,2) AS avg_diff_mins

FROM
	(
	SELECT
			ga.user_id,
			ga.session_id,
			DATETIME_DIFF(
						CAST(ga.hitStart_timestamp AS DATETIME), 
						CAST(session_timestamp AS DATETIME), second) AS time_difference_ss

	FROM (
			SELECT
			fullvisitorid AS user_id,
			visitId AS session_id,
			FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%S", TIMESTAMP_SECONDS(visitStartTime)) AS session_timestamp,
			MIN(FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%S", TIMESTAMP_MILLIS(1000 * visitStartTime + h.time))) AS hitStart_timestamp, -- when the action was recorded
						
			FROM `dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export`, UNNEST(hit) AS h
			WHERE h.eventAction = 'checkout.loaded' -- event 'order_confirmation.loaded' does not exist, used 'checkout.loaded' instead as a proxy
			AND h.isInteraction = True

			GROUP BY 1,2,3

		 ) ga
	) A
;

## SOLUTION B: another way to output results, assuming visitStartTime records event timestamp (in this case, it does not)
SELECT
A.session_date,
AVG(A.time_difference_bw_events)/3600 AS avg_time_ss

FROM (
		SELECT
		ga.fullvisitorid AS user_id,
		ga.visitId AS session_id,
		lp.session_date,
		DATETIME_DIFF(CAST(lp.session_start AS datetime), CAST(oc.confirmation_end AS datetime), second) AS time_difference_bw_events

		--> It pulls out users who started a session
		FROM `dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export` ga
		LEFT JOIN 
				(	
				SELECT
				fullvisitorid AS user_id,
				visitId AS session_id,
			  	MIN(FORMAT_TIMESTAMP("%Y-%m-%d", TIMESTAMP_SECONDS(visitStartTime))) AS session_date,
        		MIN(FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%S", TIMESTAMP_SECONDS(visitStartTime))) AS session_start
				
				FROM `dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export`, UNNEST(hit) AS h
				WHERE h.eventAction = 'shop_list.loaded'
				
				GROUP BY 1,2

				) lp ON lp.user_id = ga.fullvisitorid AND lp.session_id = ga.visitId

		--> It gets users who arrived to the confirmation/checkout page
		LEFT JOIN 
				(	
				SELECT
				fullvisitorid AS user_id,
				visitId AS session_id,
			  	MIN(FORMAT_TIMESTAMP("%Y-%m-%d", TIMESTAMP_SECONDS(visitStartTime))) AS session_date,
        		MIN(FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%S", TIMESTAMP_SECONDS(visitStartTime))) AS confirmation_end --> assumes it as event timestamp
				
				FROM `dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export`, UNNEST(hit) AS h
				WHERE h.eventAction = 'checkout.loaded' -- event 'order_confirmation.loaded' does not exist

				GROUP BY 1,2

				) oc ON oc.user_id = lp.user_id AND oc.session_id = lp.session_id AND oc.session_date = lp.session_date
		
		) A

GROUP BY 1;


##### 3. USERS CHANGING PAYMENT METHODS
# SOLUTION: 77%

SELECT 
COUNT(DISTINCT A.user_id) AS nr_users,
COUNT(DISTINCT IF(A.nr_events > 1, A.user_id, NULL)) AS nr_users_cpm,
ROUND(COUNT(DISTINCT IF(A.nr_events > 1, A.user_id, NULL))/COUNT(DISTINCT A.user_id),2) AS percent_users_cpm

FROM 
	(
	SELECT 
		(
		 SELECT visitId FROM h.customDimensions WHERE index=25
		) AS session_id,
		fullvisitorid AS user_id,
		FORMAT_TIMESTAMP("%Y-%m-%d", TIMESTAMP_SECONDS(visitStartTime)) AS session_date,
		-- FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%S", TIMESTAMP_MILLIS(1000 * visitStartTime + h.time)) AS hitStart_timestamp, # verifies unique events
		COUNT(h.eventAction) AS nr_events

		FROM `dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export`, UNNEST(hit) AS h
		WHERE h.eventAction = 'order_payment_method.chosen'

		GROUP BY 1,2,3
	) A


##### 4. CONVERSION RATE: shop list to shop details page
# SOLUTION: ~67%

SELECT
sl.session_date,
COUNT(DISTINCT sl.user_id) AS nr_users,
SUM(IF(sd.user_id IS NOT NULL, 1, 0)) AS converted_user,
ROUND(SUM(IF(sd.user_id IS NOT NULL, 1, 0))/COUNT(DISTINCT sl.user_id),2) AS conversion_rate

		--> It pulls users who started a session
FROM  
	(	
	SELECT
	fullvisitorid AS user_id,
	visitId AS session_id,
	MIN(FORMAT_TIMESTAMP("%Y-%m-%d", TIMESTAMP_SECONDS(visitStartTime))) AS session_date,

	FROM `dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export`, UNNEST(hit) AS h
	WHERE h.eventAction = 'shop_list.loaded'

	GROUP BY 1,2
	) sl

		--> It pulls users who arrived to the confirmation page
LEFT JOIN 
	(	
	SELECT
	fullvisitorid AS user_id,
	visitId AS session_id,
			  	MIN(FORMAT_TIMESTAMP("%Y-%m-%d", TIMESTAMP_SECONDS(visitStartTime))) AS session_date,
	
	FROM `dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export`, UNNEST(hit) AS h
	WHERE h.eventAction = 'shop_details.loaded'

	GROUP BY 1,2

	) sd ON sd.user_id = sl.user_id AND sd.session_id = sl.session_id AND sd.session_date = sl.session_date

GROUP BY 1
ORDER BY 1 DESC;


##### 5. TOP 10 USERS WITH LONGEST AVERAGE DAYS BETWEEN VISITS
SELECT
c.user_id, 
ROUND(AVG(diff_days),0) AS avg_diff_days

FROM
	(
	SELECT 
	*, DATE_DIFF(CAST(B.next_start_time AS date), CAST(B.session_date AS date), DAY) AS diff_days

	FROM
		(
		SELECT
		A.user_id, A.session_id, A.session_date,
		LEAD(A.session_date) OVER (PARTITION BY A.user_id ORDER BY A.session_date) AS next_start_time,
		-- ROW_NUMBER() OVER ( PARTITION BY A.visitStartTime ORDER BY A.fullvisitorid ASC) AS next_start_time

		FROM 
			(
			SELECT
			fullvisitorid AS user_id,
			visitId AS session_id,
			FORMAT_TIMESTAMP("%Y-%m-%d", TIMESTAMP_SECONDS(visitStartTime)) AS session_date,
			SUM(visits) AS nr_visits_per_session


			FROM `dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export`, UNNEST(hit) AS h

			GROUP BY 1,2,3
			HAVING SUM(visits) > 10
		  	ORDER BY 1,2,3 ASC
			) A
		) B

	WHERE B.next_start_time IS NOT NULL
	) C

GROUP BY 1
ORDER BY 2 DESC
LIMIT 10; --> additionally, dense_rank() and row_number() can also be used


##### 6. SEMI-STRUCTURED TABLE
WITH `dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export` AS (
	SELECT
	A.platform, A.yyyy_mm_dd, A.country,
	SUM(A.nr_sessions) AS nr_sessions

	FROM
		(
		SELECT
		fullvisitorId,
		FORMAT_TIMESTAMP("%Y-%m-%d", TIMESTAMP_SECONDS(visitStartTime)) AS yyyy_mm_dd, country, operatingSystem AS platform,
		COUNT(DISTINCT visitId) AS nr_sessions

		FROM `dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export`

		GROUP BY 1,2,3,4
	    ORDER BY 1,2 DESC
		) A

	GROUP BY 1,2,3
	ORDER BY 2,1,3 DESC)

SELECT j, TO_JSON_STRING(j) AS semi_str FROM `dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export` j