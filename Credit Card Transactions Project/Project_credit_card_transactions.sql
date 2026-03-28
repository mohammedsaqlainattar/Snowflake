SELECT * FROM credit_card_transactions;

--------------------------------------------------------------

-- 1- write a query to print top 5 cities with highest spends and their percentage contribution of total credit card spends

WITH city_spend AS (
SELECT
    city,
    SUM(amount) AS total_spend
FROM
    credit_card_transactions
GROUP BY city
),
ranking AS (
SELECT
    city,
    total_spend,
    DENSE_RANK() OVER(ORDER BY total_spend DESC) AS rnk,
    total_spend / (SUM(total_spend) OVER())*100 AS pct_contr
FROM city_spend
)
SELECT * FROM ranking WHERE rnk<=5;

--------------------------------------------------------------

-- 2- write a query to print highest spend month and amount spent in that month for each card type

WITH monthly_transcations AS (
SELECT 
    card_type,
    YEAR(transaction_date) AS transaction_year,
    MONTH(transaction_date) AS transaction_month,
    SUM(amount) AS amount
FROM credit_card_transactions
GROUP BY card_type, YEAR(transaction_date), MONTH(transaction_date)
),
monthly_ranking AS (
SELECT
    card_type,
    transaction_year,
    transaction_month,
    amount,
    ROW_NUMBER() OVER(PARTITION BY card_type ORDER BY amount DESC) AS rn
FROM monthly_transcations
)
SELECT 
    card_type,
    transaction_year,
    transaction_month,
    amount 
FROM monthly_ranking WHERE rn=1;

-------------------------------------------------------------------------

-- 3 Write a query to print the transaction details (all columns from the table) for each card type when
-- it reaches a cumulative of 1000000 total spends(We should have 4 rows in the o/p one for each card type)

WITH cum_spend AS (
SELECT
    *,
    SUM(amount) OVER(PARTITION BY card_type ORDER BY transaction_date, transaction_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_spend
FROM credit_card_transactions
),
flag AS (
SELECT 
    *,
    CASE WHEN cumulative_spend >= 1000000 THEN 1 ELSE 0 END AS flag_row
FROM cum_spend
),
first_Mil_record AS (
SELECT 
    *,
    flag_row - LAG(flag_row,1) OVER(PARTITION BY card_type ORDER BY cumulative_spend) AS first_M_val
FROM flag
)
SELECT * FROM first_mil_record WHERE first_M_val=1;

--Method 2:
with cte as (
select *,sum(amount) over(partition by card_type order by transaction_date,transaction_id) as total_spend
from credit_card_transactions
--order by card_type,total_spend desc
)
select * from (select *, rank() over(partition by card_type order by total_spend) as rn  
from cte where total_spend >= 1000000) a where rn=1;

--------------------------------------------------------------------

-- 4- write a query to find city which had lowest percentage spend for gold card type

WITH cte AS (
SELECT
    city,
    card_type,
    SUM(amount) AS amount
FROM credit_card_transactions
GROUP BY city, card_type
)
SELECT
    *,
    ((CASE WHEN card_type='Gold' THEN amount END) / SUM(amount) OVER(PARTITION BY city)) AS pct_gold_spend
FROM cte
ORDER BY pct_gold_spend ASC LIMIT 1;

--------------------------------------------------------

-- 5- write a query to print 3 columns:  city, highest_expense_type , lowest_expense_type (example format : Delhi , bills, Fuel)


WITH cte AS (
SELECT 
    city,
    exp_type,
    SUM(amount) AS amount
FROM credit_card_transactions
GROUP BY city, exp_type
),
exp_ranking AS (
SELECT
    *,
    DENSE_RANK() OVER(PARTITION BY city ORDER BY amount DESC) AS rnk
FROM cte
),
results AS (
SELECT
    *,
    MIN(rnk) OVER(PARTITION BY city) AS highest_exp_rnk,
    MAX(rnk) OVER(PARTITION BY city) AS lowest_exp_rnk
FROM exp_ranking
)
SELECT
    city,
    MAX((CASE WHEN rnk = highest_exp_rnk THEN exp_type END)) AS highest_exp_type,
    MAX((CASE WHEN rnk = lowest_exp_rnk THEN exp_type END)) AS lowest_exp_type
FROM results
GROUP BY city
ORDER BY city;

---------------------------------------------------------------------------

-- 6- write a query to find percentage contribution of spends by females for each expense type


WITH cte AS (
SELECT
    exp_type,
    gender,
    SUM(amount) AS amount
FROM credit_card_transactions
GROUP BY exp_type, gender
),
gender_contribution AS (
SELECT
    *,
    ROUND((amount / SUM(amount) OVER(PARTITION BY exp_type))*100,2) AS pct_contr 
FROM cte
)
SELECT exp_type, gender, pct_contr
FROM gender_contribution
WHERE gender='F'
ORDER BY exp_type;

-- Method 2:
select exp_type,
sum(case when gender='F' then amount else 0 end)*1.0/sum(amount) as percentage_female_contribution
from credit_card_transactions
group by exp_type
order by percentage_female_contribution desc;

---------------------------------------------------------------

-- 7- which card and expense type combination saw highest month over month growth in Jan-2014

WITH cte AS (
SELECT  
    YEAR(transaction_date) AS YYYY, 
    MONTH(transaction_date) AS MM,
    card_type,
    exp_type,
    SUM(amount) AS amount
FROM credit_card_transactions
GROUP BY YEAR(transaction_date),MONTH(transaction_date),card_type,exp_type
),
MoM_growth AS (
SELECT
    *,
    LAG(amount) OVER(PARTITION BY card_type,exp_type ORDER BY YYYY,MM) AS prev_month_amt
FROM cte
)
SELECT *, (amount - prev_month_amt) AS growth
FROM MoM_growth 
WHERE YYYY=2014 AND MM=1
ORDER BY growth DESC
LIMIT 1;

--------------------------------------------------------------

-- 8- during weekends which city has highest total spend to total no of transactions ratio

SELECT
    city,
    SUM(amount) AS amount,
    COUNT(transaction_id) AS transaction_cnt,
    SUM(amount) / COUNT(transaction_id) AS spend_to_transaction_cnt_ratio
FROM credit_card_transactions
WHERE DAYNAME(transaction_date) IN ('Sun','Sat')
GROUP BY city
ORDER BY spend_to_transaction_cnt_ratio DESC
LIMIT 1;

--------------------------------------------------------------

-- 9- which city took least number of days to reach its 500th transaction after the first transaction in that city


WITH transaction_no AS (
SELECT 
    city,
    transaction_date,
    ROW_NUMBER() OVER(PARTITION BY city ORDER BY transaction_date) AS rn
FROM credit_card_transactions
),
days_diff AS (
SELECT
    *,
    LAG(transaction_date) OVER(PARTITION BY city ORDER BY transaction_date) AS first_tran_date,
    DATEDIFF(DAY,LAG(transaction_date) OVER(PARTITION BY city ORDER BY transaction_date),transaction_date) AS days_elapsed
FROM transaction_no 
WHERE rn=1 OR rn=500
)
SELECT
    city,
    first_tran_date,
    transaction_date AS fivehund_th_tran_date,
    days_elapsed
FROM days_diff 
WHERE days_elapsed IS NOT NULL 
ORDER BY days_elapsed ASC
LIMIT 1;

----------------------------------------------------------------