SELECT year, 
SUM(total_earnings_female) as total_earnings_female_sum,
SUM(total_earnings_male) as total_earnings_male_sum
FROM jobs 
GROUP BY year
ORDER BY year;
