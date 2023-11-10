-- X: job
-- Y: balance
-- Z: married

-- 1. How many unique values are in variable X?
SELECT COUNT(DISTINCT job) AS unique_values_count
FROM bank_marketing;


-- 2. What is the average of variable Y grouped by variable Z?
SELECT married, AVG(balance) AS average_balance
FROM bank_marketing
GROUP BY balance
ORDER BY average_balance ASC;