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
    MONTH(transaction_date) AS transaction_month,
    SUM(amount) AS amount
FROM credit_card_transactions
GROUP BY card_type, MONTH(transaction_date)
),
monthly_ranking AS (
SELECT
    card_type,
    transaction_month,
    amount,
    ROW_NUMBER() OVER(PARTITION BY card_type ORDER BY amount DESC) AS rnk
FROM monthly_transcations
)
SELECT 
    card_type,
    transaction_month,
    amount 
FROM monthly_ranking WHERE rnk=1;

-------------------------------------------------------------------------

-- 3 Write a query to print the transaction details (all columns from the table) for each card type when
-- it reaches a cumulative of 1000000 total spends(We should have 4 rows in the o/p one for each card type)

WITH cum_spend AS (
SELECT
    *,
    SUM(amount) OVER(PARTITION BY card_type ORDER BY transaction_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_spend
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

--------------------------------------------------------------------

-- 4- write a query to find city which had lowest percentage spend for gold card type

WITH cte AS (
SELECT
    city,
    card_type,
    SUM(amount) AS amount
FROM credit_card_transactions
WHERE card_type = 'Gold'
GROUP BY city, card_type
)
SELECT
    *,
    (amount / SUM(amount) OVER())*100 AS pct_spend
FROM cte
ORDER BY pct_spend ASC LIMIT 1;

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
    city,
    exp_type,
    amount,
    rnk,
    MIN(rnk) OVER(PARTITION BY city) AS highest_exp_rnk,
    MAX(rnk) OVER(PARTITION BY city) AS lowest_exp_rnk
FROM exp_ranking
)
SELECT
    city,
    MAX((CASE WHEN rnk = highest_exp_rnk THEN exp_type END)) AS highest_exp_type,
    MAX((CASE WHEN rnk = lowest_exp_rnk THEN exp_type END)) AS lowest_exp_type
FROM results
-- WHERE rnk = highest_exp_type OR rnk = lowest_exp_type
GROUP BY city
ORDER BY city;

---------------------------------------------------------------------------

-- 6- write a query to find percentage contribution of spends by females for each expense type