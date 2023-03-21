SELECT year,
SUM(full_time_male) as full_time_male_sum,
SUM(full_time_female) as full_time_female_sum,
SUM(part_time_male) as part_time_male_sum,
SUM(part_time_female) as part_time_female_sum
FROM jobs
GROUP BY year
ORDER BY year;