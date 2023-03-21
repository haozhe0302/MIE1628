SELECT SUM(total_earnings_female)
FROM jobs
WHERE occupation LIKE '%Engineer%' AND year = 2016;