SELECT year, SUM(workers_female) as workers_female_sum
FROM jobs 
WHERE major_category = 'Management, Business, and Financial' 
GROUP BY year;